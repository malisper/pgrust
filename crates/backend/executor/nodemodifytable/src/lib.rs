// nodeModifyTable.c, single-relation INSERT/UPDATE/DELETE arms. The subplan
// stays with the ExecProcNode dispatcher (execmain owns the node enum;
// nodesort precedent) — exec_modify_table takes fetch and EvalPlanQual
// closures. AFTER ROW triggers queue via the trigger crate (RI lane);
// BEFORE/INSTEAD/statement triggers, MERGE, ON CONFLICT and FDW batching are
// loud named panics; RETURNING projects OLD/NEW-free lists (those are loud
// at projection build).
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

pub struct ModifyTableState<'mcx> {
    pub plan: &'mcx ModifyTable<'mcx>,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub mt_done: bool,
    fireBSTriggers: bool,
    result_relkind: u8,
    pub result_rti: u32,
    ri_newTupleSlot: Option<ExecSlotId>,
    ri_oldTupleSlot: Option<ExecSlotId>,
    ri_ReturningSlot: Option<ExecSlotId>,
    ri_projectNewInfoValid: bool,
    ri_RowIdAttNo: i16,
    update_cols: mcx::PgVec<'mcx, NewColSrc>,
    indexes: Option<execindexing::ResultRelIndexState<'mcx>>,
    // C's per-tuple econtext for index expression/predicate eval, reset per
    // outer row; node-owned because estate can't lend its per-tuple mcx while
    // relation/slot field borrows are live. Option: dropped in
    // exec_end_modify_table (the node struct is forgotten, never dropped).
    index_eval_cx: Option<mcx::MemoryContext>,
    snapshot_any: Option<Rc<SnapshotData<'mcx>>>,
    returning_slot: Option<ExecSlotId>,
    project_returning: Option<PgBox<'mcx, ExprState<'mcx>>>,
    on_conflict: Option<OnConflictState<'mcx>>,
    // ri_CheckConstraintExprs (built on first ExecRelCheck, per C); each
    // compiled qual rides with its constraint name for the 23514 report.
    check_exprs: Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>,
    // ri_WithCheckOptions + ri_WithCheckOptionExprs, flattened.
    wco_exprs: mcx::PgVec<'mcx, WcoExpr<'mcx>>,
    // C ri_TrigDesc; Rc clone of the relcache entry's desc (CopyTriggerDesc).
    trigdesc: Option<Rc<types_trigger::TriggerDesc<'static>>>,
    // C ri_TrigFunctions + ExecGetTriggerOldSlot.
    trig_fmgr: ::trigger::TriggerFmgrCache,
    trig_old_slot: Option<ExecSlotId>,
    // ri_GeneratedExprsI/U collapsed to one set: the UPDATE updatedCols skip
    // is perf-only (values are immutable functions of non-generated columns).
    generated_exprs: Option<mcx::PgVec<'mcx, GeneratedExpr<'mcx>>>,
    // ri_GenVirtualNotNullConstraintExprs.
    virtual_nn_exprs: Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>,
    // Partitioned-target INSERT routing (execPartition.c); per-leaf insert
    // state is indexed by the router's leaf index.
    router: Option<execpartition::PartitionTupleRouting<'mcx>>,
    leaf_indexes: Vec<Option<execindexing::ResultRelIndexState<'mcx>>>,
    leaf_checks: Vec<Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>>,
    leaf_virtual_nn: Vec<Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>>,
    merge: Option<MergeState<'mcx>>,
}

// ExecInitMerge's per-statement state: ri_MergeActions split by match kind
// (NOT MATCHED BY SOURCE is loud in the planner, so two lists) and a NULL
// ri_MergeJoinCondition (non-NULL only with BY SOURCE actions).
struct MergeState<'mcx> {
    matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>>,
    not_matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>>,
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

struct CheckExpr<'mcx> {
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
    assert_eq!(node.resultRelations.len(), 1);
    debug_assert!(node.rootRelation == 0 && node.rowMarks.is_nil());
    let rti = node.resultRelations.nth(0) as u32;
    debug_assert!(estate.es_unpruned_relids.is_member(rti as i32));

    estate.exec_init_result_relation(rti)?;
    let (trigdesc, result_relkind) = {
        let rel = estate.es_relations[(rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let td = if rel.rd_hastriggers {
            let td = relcache::RelationGetTriggerDesc(rel.rd_id)?;
            if let Some(td) = &td {
                if td.triggers.iter().any(|t| t.tgoldtable.is_some() || t.tgnewtable.is_some())
                {
                    panic!(
                        "ExecInitModifyTable (nodeModifyTable.c): transition tables \
                         unported"
                    );
                }
            }
            td
        } else {
            None
        };
        check_valid_result_rel(rel, node.operation, td.as_deref())?;
        (td, rel.rd_rel.relkind)
    };

    // The UPDATE/DELETE row identity: plain relations carry a junk ctid in
    // the subplan targetlist; views (INSTEAD OF lane) a junk wholerow.
    let mut rowid_attno: i16 = 0;
    if matches!(
        node.operation,
        CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
    ) {
        let subplan = node
            .plan
            .lefttree
            .expect("ModifyTable has a subplan")
            .as_plan()
            .expect("plan node");
        if result_relkind == types_rel::RELKIND_VIEW {
            rowid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "wholerow");
            assert!(rowid_attno > 0, "could not find junk wholerow column");
        } else {
            rowid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "ctid");
            assert!(rowid_attno > 0, "could not find junk ctid column");
        }
    }

    // The RETURNING projection: scan vars read the returned tuple (result
    // relation descriptor), OUTER_VARs the plan tuple; the result slot is
    // virtual over the caller-built descriptor.
    let mut returning_slot = None;
    let mut project_returning = None;
    if !node.returningLists.is_nil() {
        assert_eq!(node.returningLists.len(), 1);
        let rlist = node
            .returningLists
            .nth(0)
            .as_list()
            .expect("returningLists cell is a List");
        let params = estate.param_bind();
        let mcx = estate.es_query_cxt;
        let proj = {
            let rel = estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            exec_build_projection_info(mcx, rlist, Some(&rel.rd_att), params)?
        };
        let desc = returning_desc.expect("caller passes the RETURNING result descriptor");
        returning_slot =
            Some(estate.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::Virtual));
        project_returning = Some(proj);
    }

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
                let rel = estate.es_relations[(rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                exec_build_projection_info(mcx, &node.onConflictSet, Some(&rel.rd_att), params)?
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
            assert_eq!(set_attnos.len(), node.onConflictSet.len());
            if let Some(where_node) = node.onConflictWhere {
                let qual = where_node
                    .as_list()
                    .expect("onConflictWhere is an implicit-AND List after preprocessing");
                where_clause = execexpr::exec_init_qual(mcx, qual, estate.param_bind())?;
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

    let mut wco_exprs: mcx::PgVec<'mcx, WcoExpr<'mcx>> = mcx::PgVec::new_in(estate.es_query_cxt);
    if !node.withCheckOptionLists.is_nil() {
        if node.operation == CmdType::CMD_MERGE {
            panic!(
                "ExecInitModifyTable (nodeModifyTable.c): WCO_RLS_MERGE_* enforcement \
                 not wired into exec_merge_matched (C ExecMergeMatched checks them)"
            );
        }
        debug_assert_eq!(node.withCheckOptionLists.len(), node.resultRelations.len());
        let mcx = estate.es_query_cxt;
        let params = estate.param_bind();
        let wlist = node
            .withCheckOptionLists
            .nth(0)
            .as_list()
            .expect("withCheckOptionLists cell is a List");
        for wco_node in wlist {
            let wco = wco_node.as_with_check_option().expect("WCO cell");
            if wco.kind == WCOKind::WCO_VIEW_CHECK {
                panic!(
                    "ExecInitModifyTable (nodeModifyTable.c): WCO_VIEW_CHECK \
                     (views WITH CHECK OPTION lane)"
                );
            }
            let qual = wco
                .qual
                .expect("planned WCO has a qual")
                .as_list()
                .expect("WCO qual is an implicit-AND List after preprocessing");
            let state = execexpr::exec_init_qual(mcx, qual, params)?
                .expect("planner dropped constant-true WCO quals");
            wco_exprs.push(WcoExpr {
                kind: wco.kind,
                relname: wco.relname.expect("WCO relname"),
                polname: wco.polname,
                state,
            });
        }
    }
    // ExecInitMerge + ExecInitMergeTupleSlots.
    let mut merge = None;
    let mut merge_old_slot = None;
    let mut merge_new_slot = None;
    let mut merge_proj_valid = false;
    if node.operation == CmdType::CMD_MERGE {
        let mcx = estate.es_query_cxt;
        assert_eq!(node.mergeActionLists.len(), 1);
        let jc = node
            .mergeJoinConditions
            .nth(0)
            .as_list()
            .expect("mergeJoinConditions cell is a List");
        assert!(
            jc.is_nil(),
            "ExecInitMerge (nodeModifyTable.c): non-NULL merge join condition \
             (NOT MATCHED BY SOURCE) not ported"
        );
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
        let mal = node
            .mergeActionLists
            .nth(0)
            .as_list()
            .expect("mergeActionLists cell is a List");
        let params = estate.param_bind();
        for action_node in mal {
            let action = action_node.as_merge_action().expect("MergeAction cell");
            let when_qual = match action.qual {
                None => None,
                Some(q) => {
                    let ql = q.as_list().expect("preprocessed WHEN qual is a List");
                    execexpr::exec_init_qual(mcx, ql, params)?
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
                    let rel = estate.es_relations[(rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened");
                    exec_check_plan_output(rel, &action.targetList)?;
                    exec_action.proj = Some(exec_build_projection_info(
                        mcx,
                        &action.targetList,
                        Some(&rel.rd_att),
                        params,
                    )?);
                }
                CmdType::CMD_UPDATE => {
                    for tle_node in &action.targetList {
                        let tle = tle_node.as_target_entry().expect("TargetEntry");
                        assert!(
                            !tle.resjunk,
                            "ExecBuildUpdateProjection: junk entry in MERGE UPDATE \
                             action targetlist"
                        );
                    }
                    let proj = {
                        let rel = estate.es_relations[(rti - 1) as usize]
                            .as_ref()
                            .expect("result relation opened");
                        exec_build_projection_info(
                            mcx,
                            &action.targetList,
                            Some(&rel.rd_att),
                            params,
                        )?
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
                    assert_eq!(exec_action.set_attnos.len(), action.targetList.len());
                }
                CmdType::CMD_DELETE | CmdType::CMD_NOTHING => {}
                other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
            }
            use types_nodes::MergeMatchKind::*;
            match action.matchKind {
                MERGE_WHEN_MATCHED => matched_actions.push(exec_action),
                MERGE_WHEN_NOT_MATCHED_BY_TARGET => not_matched_actions.push(exec_action),
                MERGE_WHEN_NOT_MATCHED_BY_SOURCE => panic!(
                    "ExecInitMerge (nodeModifyTable.c): NOT MATCHED BY SOURCE \
                     action not ported"
                ),
            }
        }
        merge = Some(MergeState { matched_actions, not_matched_actions });
    }

    Ok(ModifyTableState {
        plan: node,
        operation: node.operation,
        canSetTag: node.canSetTag,
        mt_done: false,
        fireBSTriggers: true,
        result_relkind,
        result_rti: rti,
        ri_newTupleSlot: merge_new_slot,
        ri_oldTupleSlot: merge_old_slot,
        ri_ReturningSlot: None,
        ri_projectNewInfoValid: merge_proj_valid,
        ri_RowIdAttNo: rowid_attno,
        update_cols: mcx::PgVec::new_in(estate.es_query_cxt),
        indexes: None,
        index_eval_cx: Some(mcx::MemoryContext::new_bump("IndexEvalPerTuple")),
        snapshot_any: Some(Rc::new(SnapshotData::sentinel(estate.es_query_cxt, SNAPSHOT_ANY))),
        returning_slot,
        project_returning,
        on_conflict,
        check_exprs: None,
        wco_exprs,
        trigdesc,
        trig_fmgr: ::trigger::TriggerFmgrCache::default(),
        trig_old_slot: None,
        generated_exprs: None,
        virtual_nn_exprs: None,
        router: None,
        leaf_indexes: Vec::new(),
        leaf_checks: Vec::new(),
        leaf_virtual_nn: Vec::new(),
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

// CheckValidResultRel (execMain.c), plain-table arm.
fn check_valid_result_rel(
    rel: &Relation<'_>,
    operation: CmdType,
    trigdesc: Option<&types_trigger::TriggerDesc<'static>>,
) -> PgResult<()> {
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        if operation != CmdType::CMD_INSERT {
            panic!(
                "ExecInitModifyTable: {operation:?} on a partitioned table                  (inherited result relations) not ported"
            );
        }
        return Ok(());
    }
    if rel.rd_rel.relkind == types_rel::RELKIND_VIEW {
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
    if rel.rd_rel.relkind != RELKIND_RELATION {
        panic!(
            "CheckValidResultRel (execMain.c): relkind '{}' result relation not ported",
            rel.rd_rel.relkind as char
        );
    }
    if rel.rd_rel.relispartition
        && matches!(operation, CmdType::CMD_INSERT | CmdType::CMD_UPDATE)
    {
        panic!(
            "ExecPartitionCheck (execPartition.c): direct {operation:?} into a              partition not ported (route via the parent)"
        );
    }
    Ok(())
}

// error_view_not_updatable (rewriteHandler.c), executor-check leg (no
// errdetail, per C's CheckValidResultRel call).
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

/// `ExecModifyTable` (nodeModifyTable.c), INSERT/UPDATE/DELETE loop.
/// `epq_eval` is execMain's `EvalPlanQual` over the caller-owned EPQState
/// (input = the locked latest row version in the EvalPlanQualSlot).
pub fn exec_modify_table<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: impl FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    mut epq_eval: impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    if mt.mt_done {
        return Ok(None);
    }
    if mt.fireBSTriggers {
        fire_bs_triggers(mt, estate)?;
        mt.fireBSTriggers = false;
    }

    loop {
        estate.reset_per_tuple_expr_context();
        mt.index_eval_cx.as_mut().expect("index_eval_cx live until ExecEndNode").reset();

        let Some(plan_slot) = fetch_outer(estate)? else {
            break;
        };

        match mt.operation {
            CmdType::CMD_INSERT => {
                if !mt.ri_projectNewInfoValid {
                    exec_init_insert_projection(mt, estate)?;
                }
                let slot = exec_get_insert_new_tuple(mt, estate, plan_slot)?;
                let result = exec_insert(mt, estate, slot, &mut epq_eval)?;
                if let Some(rslot) = result {
                    if mt.project_returning.is_some() {
                        return Ok(Some(exec_process_returning(mt, estate, rslot, plan_slot)?));
                    }
                }
            }
            CmdType::CMD_UPDATE if mt.result_relkind == types_rel::RELKIND_VIEW => {
                let old_tup = fetch_wholerow_tuple(mt, estate, plan_slot)?;
                if !mt.ri_projectNewInfoValid {
                    exec_init_update_projection(mt, estate)?;
                }
                let old_slot = mt.ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
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
                    if mt.canSetTag {
                        estate.es_processed += 1;
                    }
                    if mt.project_returning.is_some() {
                        return Ok(Some(exec_process_returning(mt, estate, slot, plan_slot)?));
                    }
                }
            }
            CmdType::CMD_UPDATE => {
                let mut tupleid = fetch_row_id(mt, estate, plan_slot);
                if !mt.ri_projectNewInfoValid {
                    exec_init_update_projection(mt, estate)?;
                }
                fetch_old_row_version(mt, estate, &tupleid)?;
                let slot = exec_get_update_new_tuple(mt, estate, plan_slot)?;
                let modified = exec_update(mt, estate, &mut tupleid, slot, &mut epq_eval)?;
                if modified && mt.project_returning.is_some() {
                    return Ok(Some(exec_process_returning(mt, estate, slot, plan_slot)?));
                }
            }
            CmdType::CMD_DELETE if mt.result_relkind == types_rel::RELKIND_VIEW => {
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
                    if mt.project_returning.is_some() {
                        return Ok(Some(exec_process_returning(
                            mt, estate, old_slot, plan_slot,
                        )?));
                    }
                }
            }
            CmdType::CMD_DELETE => {
                let mut tupleid = fetch_row_id(mt, estate, plan_slot);
                let modified = exec_delete(mt, estate, &mut tupleid, &mut epq_eval)?;
                if modified && mt.project_returning.is_some() {
                    let old_slot = exec_delete_fetch_old(mt, estate, &tupleid)?;
                    return Ok(Some(exec_process_returning(mt, estate, old_slot, plan_slot)?));
                }
            }
            CmdType::CMD_MERGE => {
                let tupleid = fetch_merge_row_id(mt, estate, plan_slot);
                if let Some(rslot) = exec_merge(mt, estate, plan_slot, tupleid, &mut epq_eval)? {
                    return Ok(Some(rslot));
                }
            }
            other => panic!("ExecModifyTable (nodeModifyTable.c): {other:?} arm not ported"),
        }
    }

    debug_assert!(estate.es_insert_pending_result_relations.is_empty());
    fire_as_triggers(mt, estate)?;
    mt.mt_done = true;
    Ok(None)
}

// fireBSTriggers/fireASTriggers (nodeModifyTable.c); INSERT ... ON CONFLICT
// DO UPDATE fires both INSERT and UPDATE statement triggers (AS: UPDATE
// first); MERGE fires per present subcommand.
fn fire_bs_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use types_trigger::*;
    if mt.trigdesc.is_none() {
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

fn fire_as_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(td) = mt.trigdesc.clone() else {
        return Ok(());
    };
    let (ins, upd, del) = stmt_trigger_ops(mt, false);
    let rel = estate.es_relations[(mt.result_rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    if del {
        ::trigger::ExecASDeleteTriggers(rel, &td)?;
    }
    if upd {
        ::trigger::ExecASUpdateTriggers(rel, &td)?;
    }
    if ins {
        ::trigger::ExecASInsertTriggers(rel, &td)?;
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
            if let Some(m) = &mt.merge {
                for a in m.matched_actions.iter().chain(m.not_matched_actions.iter()) {
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
    let trigdesc = mt.trigdesc.as_ref().expect("caller checked trigdesc").clone();
    let has_before = match event_op {
        types_trigger::TRIGGER_EVENT_INSERT => trigdesc.trig_insert_before_statement,
        types_trigger::TRIGGER_EVENT_UPDATE => trigdesc.trig_update_before_statement,
        _ => trigdesc.trig_delete_before_statement,
    };
    if !has_before {
        return Ok(());
    }
    let relid = {
        let rel = estate.es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        rel.rd_id
    };
    if ::trigger::before_stmt_triggers_fired(relid, event_op) {
        return Ok(());
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
            panic!(
                "TriggerEnabled (trigger.c): WHEN clause / UPDATE OF columns \
                 unported on the BEFORE STATEMENT path"
            );
        }
        let ret = {
            let finfo = mt.trig_fmgr.get(i, trigger.tgfoid)?;
            let rel = estate.es_relations[(mt.result_rti - 1) as usize]
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
    debug_assert!(mt.ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.ri_RowIdAttNo as i32, &mut isnull);
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
    debug_assert!(mt.ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.ri_RowIdAttNo as i32, &mut isnull);
    if isnull {
        return None;
    }
    // SAFETY: a tid datum is a pointer to an ItemPointerData inside the
    // deformed plan tuple, live for this row.
    Some(unsafe { *(datum.as_usize() as *const ItemPointerData) })
}

// ExecMerge (nodeModifyTable.c). mt_merge_pending_not_matched is unreachable
// on this lane: it needs BY SOURCE + BY TARGET actions with RETURNING.
fn exec_merge<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    tupleid: Option<ItemPointerData>,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let mut rslot = None;
    let mut matched = tupleid.is_some();
    if let Some(mut tid) = tupleid {
        rslot = exec_merge_matched(mt, estate, plan_slot, &mut tid, &mut matched, epq_eval)?;
    }
    if !matched {
        debug_assert!(rslot.is_none());
        rslot = exec_merge_not_matched(mt, estate, plan_slot, epq_eval)?;
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
    matched: &mut bool,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(*matched);
    if mt.merge.as_ref().expect("merge state").matched_actions.is_empty() {
        return Ok(None);
    }
    fetch_old_row_version(mt, estate, tupleid)?;

    loop {
        match exec_merge_matched_scan(mt, estate, plan_slot, tupleid, matched, epq_eval)? {
            MergeMatchedOutcome::Done(rslot) => return Ok(rslot),
            MergeMatchedOutcome::Restart => continue,
            MergeMatchedOutcome::NotMatched => {
                *matched = false;
                return Ok(None);
            }
        }
    }
}

// One pass over the MATCHED action list (the lmerge_matched body).
fn exec_merge_matched_scan<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    tupleid: &mut ItemPointerData,
    matched: &mut bool,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<MergeMatchedOutcome> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let old_id = mt.ri_oldTupleSlot.expect("ExecInitMergeTupleSlots ran");
    let new_id = mt.ri_newTupleSlot.expect("ExecInitMergeTupleSlots ran");

    let n_actions = mt.merge.as_ref().expect("merge state").matched_actions.len();
    for ai in 0..n_actions {
        // WHEN [MATCHED] AND qual: scan = old target tuple, inner = plan row.
        let (command_type, pass) = {
            let merge = mt.merge.as_mut().expect("merge state");
            let action = &mut merge.matched_actions[ai];
            let EStateData { es_tupleTable, .. } = &mut *estate;
            let (o, p) = (old_id.0 as usize, plan_slot.0 as usize);
            assert!(o != p && o < es_tupleTable.len() && p < es_tupleTable.len());
            let base = es_tupleTable.as_mut_ptr();
            // SAFETY: distinct in-bounds indices of one live slice.
            let (old_slot, plan) = unsafe { (&mut *base.add(o), &mut *base.add(p)) };
            let mut slots = EvalSlots { scan: Some(old_slot), inner: Some(plan), outer: None };
            (
                action.command_type,
                execexpr::exec_qual(action.when_qual.as_deref_mut(), &mut slots)?,
            )
        };
        if !pass {
            continue;
        }

        let mut tmfd = TM_FailureData::default();
        let result = match command_type {
            CmdType::CMD_UPDATE => {
                merge_project_update(mt, estate, ai, plan_slot)?;
                merge_update_act(mt, estate, tupleid, new_id, &mut tmfd)?
            }
            CmdType::CMD_DELETE => merge_delete_act(mt, estate, tupleid, &mut tmfd)?,
            CmdType::CMD_NOTHING => TM_Result::TM_Ok,
            other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
        };

        match result {
            TM_Result::TM_Ok => {
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
                // Concurrent update: lock the latest version and re-run the
                // join via EvalPlanQual (was_matched is always true here).
                let inputslot = eval_plan_qual_slot(mt, estate);
                let lock_result = {
                    let EStateData { es_relations, es_tupleTable, es_snapshot, .. } =
                        &mut *estate;
                    let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                    let rel = es_relations[(mt.result_rti - 1) as usize]
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
                        let Some(epqslot) = epq_eval(estate, inputslot)? else {
                            // Inner join no longer matches and there are no
                            // NOT MATCHED actions reachable through it.
                            return Ok(MergeMatchedOutcome::Done(None));
                        };
                        let mut isnull = false;
                        let _ = exectuples::slot_getattr(
                            &mut estate.es_tupleTable[epqslot.0 as usize],
                            mt.ri_RowIdAttNo as i32,
                            &mut isnull,
                        );
                        if isnull {
                            // Join quals no longer pass: NOT MATCHED now.
                            return Ok(MergeMatchedOutcome::NotMatched);
                        }
                        fetch_old_row_version(mt, estate, tupleid)?;
                        debug_assert!(*matched);
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
        if mt.project_returning.is_some() {
            rslot = match command_type {
                CmdType::CMD_UPDATE => {
                    Some(exec_process_returning(mt, estate, new_id, plan_slot)?)
                }
                CmdType::CMD_DELETE => {
                    Some(exec_process_returning(mt, estate, old_id, plan_slot)?)
                }
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
    plan_slot: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let old_id = mt.ri_oldTupleSlot.expect("merge slots");
    let new_id = mt.ri_newTupleSlot.expect("merge slots");
    let merge = mt.merge.as_mut().expect("merge state");
    let action = &mut merge.matched_actions[action_idx];
    let setvals_id = action.setvals_slot.expect("UPDATE action state");

    {
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

// ExecUpdateAct + ExecUpdateEpilogue for a MERGE UPDATE action; unlike
// exec_update the TM_Result flows back so lmerge_matched drives the retry.
fn merge_update_act<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
    slot_id: ExecSlotId,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let mut lockmode = LockTupleMode::LockTupleExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;

    let result = {
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            exec_compute_stored_generated(mcx, &mut mt.generated_exprs, rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        if rel.rd_rel.relhasindex && mt.indexes.is_none() {
            mt.indexes = Some(execindexing::ExecOpenIndices(mcx, rel, false)?);
        }

        exec_constraints(mcx, &mut mt.check_exprs, &mut mt.virtual_nn_exprs, rel, slot)?;

        tableam::table_tuple_update(
            mcx,
            rel,
            tupleid,
            slot,
            output_cid,
            snapshot,
            &None,
            true,
            tmfd,
            &mut lockmode,
            &mut update_indexes,
        )?
    };
    if result != TM_Result::TM_Ok {
        return Ok(result);
    }

    let EStateData { es_relations, es_tupleTable, .. } = estate;
    let rel = es_relations[(mt.result_rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let slot = &mut es_tupleTable[slot_id.0 as usize];
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    if let Some(indexes) = mt.indexes.as_mut() {
        if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
            if update_indexes == TU_UpdateIndexes::TU_Summarizing {
                panic!(
                    "ExecUpdateEpilogue (nodeModifyTable.c): onlySummarizing \
                     index maintenance (BRIN lane) not ported"
                );
            }
            recheck_indexes = execindexing::ExecInsertIndexTuples(
                mcx,
                mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                indexes,
                rel,
                slot,
                false,
                None,
                &[],
            )?;
        }
    }
    if let Some(td) = &mt.trigdesc {
        ::trigger::ExecARUpdateTriggers(mcx, rel, td, *tupleid, slot.base().tts_tid, &recheck_indexes)?;
    }
    Ok(TM_Result::TM_Ok)
}

// ExecDeleteAct + ExecDeleteEpilogue for a MERGE DELETE action.
fn merge_delete_act<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let result = {
        let EStateData { es_relations, es_snapshot, .. } = &*estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = es_snapshot;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        tableam::table_tuple_delete(
            mcx, rel, tupleid, output_cid, snapshot, &None, true, tmfd, false,
        )?
    };
    if result != TM_Result::TM_Ok {
        return Ok(result);
    }
    if let Some(td) = &mt.trigdesc {
        let EStateData { es_relations, es_query_cxt, .. } = &*estate;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        ::trigger::ExecARDeleteTriggers(*es_query_cxt, rel, td, *tupleid)?;
    }
    Ok(TM_Result::TM_Ok)
}

// ExecMergeNotMatched (nodeModifyTable.c): first qualifying NOT MATCHED [BY
// TARGET] action; INSERT projects from the source row alone (no scan tuple).
fn exec_merge_not_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let new_id = mt.ri_newTupleSlot.expect("ExecInitMergeTupleSlots ran");
    let n_actions = mt.merge.as_ref().expect("merge state").not_matched_actions.len();
    for ai in 0..n_actions {
        let (command_type, pass) = {
            let merge = mt.merge.as_mut().expect("merge state");
            let action = &mut merge.not_matched_actions[ai];
            let plan = &mut estate.es_tupleTable[plan_slot.0 as usize];
            let mut slots = EvalSlots { scan: None, inner: Some(plan), outer: None };
            (
                action.command_type,
                execexpr::exec_qual(action.when_qual.as_deref_mut(), &mut slots)?,
            )
        };
        if !pass {
            continue;
        }
        match command_type {
            CmdType::CMD_INSERT => {
                {
                    let merge = mt.merge.as_mut().expect("merge state");
                    let action = &mut merge.not_matched_actions[ai];
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
                let inserted = exec_insert(mt, estate, new_id, epq_eval)?;
                if let Some(islot) = inserted {
                    if mt.project_returning.is_some() {
                        return Ok(Some(exec_process_returning(
                            mt, estate, islot, plan_slot,
                        )?));
                    }
                }
            }
            CmdType::CMD_NOTHING => {}
            other => panic!("unknown action in MERGE WHEN NOT MATCHED clause: {other:?}"),
        }
        return Ok(None);
    }
    Ok(None)
}

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
    if let Some(indexes) = mt.indexes.take() {
        execindexing::ExecCloseIndices(indexes).expect("ExecCloseIndices");
    }
    mt.snapshot_any = None;
    mt.project_returning = None;
    mt.on_conflict = None;
    mt.check_exprs = None;
    mt.wco_exprs.clear();
    mt.trigdesc = None;
    mt.trig_fmgr = ::trigger::TriggerFmgrCache::default();
    mt.generated_exprs = None;
    mt.virtual_nn_exprs = None;
    mt.merge = None;
    // ExecCleanupTupleRouting: close routed leaves (Relation Drop = NoLock
    // close, lock kept to commit as C) and their per-leaf insert state.
    for idx in mt.leaf_indexes.iter_mut() {
        if let Some(indexes) = idx.take() {
            execindexing::ExecCloseIndices(indexes).expect("ExecCloseIndices");
        }
    }
    mt.leaf_indexes.clear();
    mt.leaf_checks.clear();
    mt.router = None;
    mt.index_eval_cx = None;
}

// ExecInitInsertProjection (nodeModifyTable.c). INSERT subplans carry no junk
// columns on this lane (loud below), so need_projection is always false and
// ri_newTupleSlot only exists for slot-type coercion.
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
        let rel = estate.es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        exec_check_plan_output(rel, &subplan.targetlist)?;
        (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
    };
    let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(slot);
    mt.ri_newTupleSlot = Some(id);
    mt.ri_projectNewInfoValid = true;
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
        if !att.attisdropped {
            let exprtype = expr_type(tle.expr);
            if exprtype != att.atttypid {
                return Err(plan_output_mismatch(
                    "Table has a column of one type at a position where the \
                     query expects another type.",
                ));
            }
        } else if tle.expr.node_tag() != NodeTag::T_Const
            || !tle.expr.as_const().unwrap().constisnull
        {
            return Err(plan_output_mismatch(
                "Query provides a value for a dropped column.",
            ));
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
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_ScalarArrayOpExpr => 16,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttype,
        NodeTag::T_RowCompareExpr => 16,
        other => panic!("ExecCheckPlanOutput exprType arm for {other:?} not ported"),
    }
}

// ExecGetInsertNewTuple (nodeModifyTable.c), no-projection arm.
fn exec_get_insert_new_tuple<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let new_slot = mt.ri_newTupleSlot.expect("ExecInitInsertProjection ran");
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
    let update_colnos = mt
        .plan
        .updateColnosLists
        .nth(0)
        .as_int_list()
        .expect("updateColnosLists cell is an IntList");

    let mcx = estate.es_query_cxt;
    let (kind, desc) = {
        let rel = estate.es_relations[(mt.result_rti - 1) as usize]
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
    mt.update_cols = cols;

    let old_slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc.clone()));
    let old_id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(old_slot);
    let new_slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let new_id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(new_slot);
    mt.ri_oldTupleSlot = Some(old_id);
    mt.ri_newTupleSlot = Some(new_id);
    mt.ri_projectNewInfoValid = true;
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
    let new_id = mt.ri_newTupleSlot.expect("ExecInitUpdateProjection ran");
    let old_id = mt.ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
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
        for (i, src) in mt.update_cols.iter().enumerate() {
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
    let old_slot = mt.ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
    let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = estate;
    let rel = es_relations[(mt.result_rti - 1) as usize]
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
fn eval_plan_qual_slot<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ExecSlotId {
    let rti = mt.result_rti;
    estate.epq_ensure(rti);
    let idx = (rti - 1) as usize;
    if let Some(id) = estate.es_epq.as_ref().expect("just ensured").relsubs_slot[idx] {
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
    estate.es_epq.as_mut().expect("just ensured").relsubs_slot[idx] = Some(id);
    id
}

// ExecUpdate + ExecUpdatePrologue/Act/Epilogue (nodeModifyTable.c), plain-heap
// arm: no triggers/FDW/partitions. Concurrent TM_Updated runs the EPQ
// recheck (redo_act loop); the ri_needLockTagTuple relock is omitted —
// inplace-update catalogs never reach this executor path.
fn exec_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<bool> {
    let output_cid = estate.es_output_cid;
    let mut slot_id = slot_id;
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleExclusive;

    if mt.trigdesc.as_ref().is_some_and(|td| td.trig_update_before_row) {
        let Some(old_slot) = get_tuple_for_trigger(mt, estate, tupleid)? else {
            return Ok(false);
        };
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_UPDATE,
            types_trigger::TRIGGER_EVENT_UPDATE,
            Some(old_slot),
            Some(slot_id),
        )? {
            return Ok(false);
        }
    }
    let mut update_indexes = TU_UpdateIndexes::TU_None;

    // redo_act:
    loop {
        let mcx = estate.es_query_cxt;
        let result = {
            let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
            let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
            let rel = es_relations[(mt.result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let slot = &mut es_tupleTable[slot_id.0 as usize];

            slot.base_mut().tts_tableOid = rel.rd_id;
            if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
                exec_compute_stored_generated(mcx, &mut mt.generated_exprs, rel, slot)?;
            }
            exectuples::exec_materialize_slot(slot, mcx)?;
            slot.base_mut().tts_tableOid = rel.rd_id;

            if rel.rd_rel.relhasindex && mt.indexes.is_none() {
                mt.indexes = Some(execindexing::ExecOpenIndices(mcx, rel, false)?);
            }

            if !mt.wco_exprs.is_empty() {
                if rel.rd_rel.relispartition {
                    panic!(
                        "ExecUpdate: WCOs on a partition (cross-partition move \
                         check) not ported"
                    );
                }
                exec_with_check_options(&mut mt.wco_exprs, WCOKind::WCO_RLS_UPDATE_CHECK, slot)?;
            }
            exec_constraints(mcx, &mut mt.check_exprs, &mut mt.virtual_nn_exprs, rel, slot)?;

            tableam::table_tuple_update(
                mcx,
                rel,
                tupleid,
                slot,
                output_cid,
                snapshot,
                &None,
                true,
                &mut tmfd,
                &mut lockmode,
                &mut update_indexes,
            )?
        };

        match result {
            TM_Result::TM_Ok => break,
            TM_Result::TM_SelfModified => {
                if tmfd.cmax != output_cid {
                    return Err(self_modified_violation("updated"));
                }
                return Ok(false);
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
                    let rel = es_relations[(mt.result_rti - 1) as usize]
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
                        let Some(epqslot) = epq_eval(estate, inputslot)? else {
                            return Ok(false);
                        };
                        debug_assert!(mt.ri_projectNewInfoValid);
                        fetch_old_row_version(mt, estate, tupleid)?;
                        slot_id = exec_get_update_new_tuple(mt, estate, epqslot)?;
                        continue;
                    }
                    TM_Result::TM_Deleted => return Ok(false),
                    TM_Result::TM_SelfModified => {
                        if tmfd.cmax != output_cid {
                            return Err(self_modified_violation("updated"));
                        }
                        return Ok(false);
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
                return Ok(false);
            }
            other => panic!("ExecUpdate (nodeModifyTable.c): unexpected {other:?}"),
        }
    }

    let mcx = estate.es_query_cxt;
    let EStateData { es_relations, es_tupleTable, .. } = estate;
    let rel = es_relations[(mt.result_rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let slot = &mut es_tupleTable[slot_id.0 as usize];
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    if let Some(indexes) = mt.indexes.as_mut() {
        if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
            if update_indexes == TU_UpdateIndexes::TU_Summarizing {
                panic!(
                    "ExecUpdateEpilogue (nodeModifyTable.c): onlySummarizing \
                     index maintenance (BRIN lane) not ported"
                );
            }
            recheck_indexes = execindexing::ExecInsertIndexTuples(
                mcx,
                mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                indexes,
                rel,
                slot,
                false,
                None,
                &[],
            )?;
        }
    }

    if let Some(td) = &mt.trigdesc {
        ::trigger::ExecARUpdateTriggers(mcx, rel, td, *tupleid, slot.base().tts_tid, &recheck_indexes)?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(true)
}

// ExecDelete + ExecDeletePrologue/Act/Epilogue (nodeModifyTable.c), plain-heap
// arm; concurrent TM_Updated runs the EPQ recheck (ldelete loop).
fn exec_delete<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<bool> {
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();

    if mt.trigdesc.as_ref().is_some_and(|td| td.trig_delete_before_row) {
        let Some(old_slot) = get_tuple_for_trigger(mt, estate, tupleid)? else {
            return Ok(false);
        };
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_DELETE,
            types_trigger::TRIGGER_EVENT_DELETE,
            Some(old_slot),
            None,
        )? {
            return Ok(false);
        }
    }

    // ldelete:
    loop {
        let mcx = estate.es_query_cxt;
        let result = {
            let EStateData { es_relations, es_snapshot, .. } = &*estate;
            let snapshot: &tableam_vocab::Snapshot<'mcx> = es_snapshot;
            let rel = es_relations[(mt.result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            tableam::table_tuple_delete(
                mcx,
                rel,
                tupleid,
                output_cid,
                snapshot,
                &None,
                true,
                &mut tmfd,
                false,
            )?
        };

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
                let inputslot = eval_plan_qual_slot(mt, estate);
                let lock_result = {
                    let EStateData { es_relations, es_tupleTable, es_snapshot, .. } =
                        &mut *estate;
                    let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                    let rel = es_relations[(mt.result_rti - 1) as usize]
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
                        // epqreturnslot only exists on the cross-partition
                        // UPDATE path (loud at init).
                        if epq_eval(estate, inputslot)?.is_none() {
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

    if let Some(td) = &mt.trigdesc {
        let EStateData { es_relations, es_query_cxt, .. } = &*estate;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        ::trigger::ExecARDeleteTriggers(*es_query_cxt, rel, td, *tupleid)?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(true)
}

#[cold]
#[inline(never)]
fn serialization_conflict(kind: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not serialize access due to concurrent {kind}"))
            .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE),
    )
}

// ExecDelete's RETURNING arm: re-fetch the deleted tuple under SnapshotAny
// into a lazily-built table-format slot (C ExecGetReturningSlot).
fn exec_delete_fetch_old<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
) -> PgResult<ExecSlotId> {
    if mt.ri_ReturningSlot.is_none() {
        let mcx = estate.es_query_cxt;
        let (kind, desc) = {
            let rel = estate.es_relations[(mt.result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
        estate.es_tupleTable.push(slot);
        mt.ri_ReturningSlot = Some(id);
    }
    let slot_id = mt.ri_ReturningSlot.expect("just initialized");
    let found = {
        let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = estate;
        let rel = es_relations[(mt.result_rti - 1) as usize]
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
fn slot_raw_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<(*const u8, u32, ItemPointerData, types_core::Oid)> {
    let mcx = estate.es_query_cxt;
    let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
    let fetched = exectuples::exec_fetch_slot_heap_tuple(slot, true, mcx, mcx)?;
    Ok(match fetched {
        exectuples::FetchedHeapTuple::Slot(t) => {
            (t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
        }
        exectuples::FetchedHeapTuple::Copied(t) => {
            (t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
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
) -> PgResult<bool> {
    row_triggers_common(mt, estate, tgtype_event, event_op, old_slot, new_slot, false)
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
    row_triggers_common(mt, estate, tgtype_event, event_op, old_slot, new_slot, true)
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
) -> PgResult<bool> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSTEAD, TRIGGER_EVENT_ROW,
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_ROW,
        TRIGGER_TYPE_TIMING_MASK,
    };
    let mcx = estate.es_query_cxt;
    let raw_old = match old_slot {
        Some(id) => Some(slot_raw_tuple(estate, id)?),
        None => None,
    };
    let mut raw_new = match new_slot {
        Some(id) => Some(slot_raw_tuple(estate, id)?),
        None => None,
    };
    let trigdesc = mt.trigdesc.as_ref().expect("BR caller checked trigdesc").clone();
    let (type_timing, event_timing) = if instead {
        (TRIGGER_TYPE_INSTEAD, TRIGGER_EVENT_INSTEAD)
    } else {
        (TRIGGER_TYPE_BEFORE, TRIGGER_EVENT_BEFORE)
    };
    let tg_event = event_op | TRIGGER_EVENT_ROW | event_timing;
    let is_delete = event_op == TRIGGER_EVENT_DELETE;
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | tgtype_event)
            != TRIGGER_TYPE_ROW | type_timing | tgtype_event
        {
            continue;
        }
        if !::trigger::TriggerEnabled(trigger) {
            continue;
        }
        if trigger.tgnattr > 0 || trigger.tgqual.is_some() {
            panic!(
                "TriggerEnabled (trigger.c): WHEN clause / UPDATE OF columns \
                 unported on the BEFORE ROW path"
            );
        }
        // SAFETY (both): materialized query-context images; the slots are not
        // written while these handles live within this iteration.
        let mut old_t = raw_old.map(|(img, len, tid, oid)| unsafe {
            types_tuple::HeapTupleData::from_raw_parts(img, len, tid, oid)
        });
        let mut new_t = raw_new.map(|(img, len, tid, oid)| unsafe {
            types_tuple::HeapTupleData::from_raw_parts(img, len, tid, oid)
        });
        // C: INSERT/DELETE put the affected row in tg_trigtuple; UPDATE
        // carries old in tg_trigtuple and new in tg_newtuple.
        let old_nn = old_t.as_mut().map(core::ptr::NonNull::from);
        let new_nn = new_t.as_mut().map(core::ptr::NonNull::from);
        let (trig_nn, newtup_nn) =
            if old_nn.is_some() { (old_nn, new_nn) } else { (new_nn, None) };
        let expected = if newtup_nn.is_some() { newtup_nn } else { trig_nn };
        let ret = {
            let finfo = mt.trig_fmgr.get(i, trigger.tgfoid)?;
            let rel = estate.es_relations[(mt.result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let mut tdata = types_trigger_call::TriggerData::from_raw(
                tg_event, rel, trig_nn, newtup_nn, trigger,
            );
            ::trigger::ExecCallTriggerFunc(mcx, &mut tdata, finfo)?
        };
        match ret {
            None => return Ok(false),
            Some(p) if Some(p) == expected => {}
            Some(_) if is_delete => {}
            Some(p) => {
                // ExecBR{Insert,Update}Triggers replacement-tuple arm:
                // ExecForceStoreHeapTuple into the new slot, subsequent
                // triggers and the DML proper see the replaced row.
                if trigger.tgisclone {
                    panic!(
                        "ExecBRInsertTriggers (trigger.c): replacement tuple in a \
                         partition (ExecPartitionCheck re-verify) unported"
                    );
                }
                let slot_id = new_slot.expect("insert/update BR has a new slot");
                // SAFETY: p is the trigger's returned tuple, live in the
                // per-call context; copied into the slot before reuse.
                let returned = unsafe { p.as_ref() };
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
                raw_new = Some(slot_raw_tuple(estate, slot_id)?);
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
    if mt.trig_old_slot.is_none() {
        let mcx = estate.es_query_cxt;
        let (kind, desc) = {
            let rel = estate.es_relations[(mt.result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
        estate.es_tupleTable.push(slot);
        mt.trig_old_slot = Some(id);
    }
    mt.trig_old_slot.expect("just initialized")
}

// The wholerow-junk row identity of views (nodeModifyTable.c:4409-4470):
// rebuild the OLD view row; t_self invalid, t_tableOid invalid (historical
// view-trigger behavior).
fn fetch_wholerow_tuple<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<types_tuple::HeapTupleData<'mcx>> {
    debug_assert!(mt.ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.ri_RowIdAttNo as i32, &mut isnull);
    assert!(!isnull, "wholerow is NULL");
    let hdr = datum.as_usize() as *const u8;
    // SAFETY: a composite datum is an in-memory HeapTupleHeader image
    // (RowExpr output, never toasted); live in the plan slot for this row.
    let t_len = unsafe {
        (*(hdr as *const types_tuple::htup::HeapTupleHeaderData)).datum_length()
    };
    let mut tid = ItemPointerData::default();
    ItemPointerSetInvalid(&mut tid);
    // SAFETY: image bounds established above.
    Ok(unsafe {
        types_tuple::HeapTupleData::from_raw_parts(hdr, t_len, tid, types_core::InvalidOid)
    })
}

fn get_tuple_for_trigger<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
) -> PgResult<Option<ExecSlotId>> {
    let slot_id = ensure_trig_old_slot(mt, estate);
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();
    let lock_result = {
        let mcx = estate.es_query_cxt;
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.result_rti - 1) as usize]
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
            Ok(None)
        }
        TM_Result::TM_Ok => {
            if tmfd.traversed {
                panic!(
                    "GetTupleForTrigger (trigger.c): EPQ recheck after a \
                     concurrent update unported on the BEFORE ROW path"
                );
            }
            Ok(Some(slot_id))
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
            Ok(None)
        }
        other => panic!("GetTupleForTrigger (trigger.c): unrecognized status {other:?}"),
    }
}

// ExecProcessReturning (nodeModifyTable.c): scan slot = the returned tuple,
// outer slot = the plan tuple, projected into the node's virtual result slot
// (C's econtext scantuple/outertuple + ExecProject).
fn exec_process_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tuple_slot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let result_id = mt.returning_slot.expect("RETURNING slot initialized");
    let state = mt.project_returning.as_deref_mut().expect("RETURNING projection built");
    let mcx = estate.es_query_cxt;
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    let (t, p, r) = (tuple_slot.0 as usize, plan_slot.0 as usize, result_id.0 as usize);
    assert!(t < table.len() && p < table.len() && r < table.len());
    assert!(r != t && r != p);
    let base = table.as_mut_ptr();
    // SAFETY: bounds-checked, result distinct from both inputs; when the plan
    // slot IS the tuple slot (INSERT without slot coercion) only one &mut is
    // derived and OUTER_VAR references panic loudly in the interpreter.
    let scan = unsafe { &mut *base.add(t) };
    // SAFETY: as above; p != t makes the borrows disjoint.
    let outer = if p != t { Some(unsafe { &mut *base.add(p) }) } else { None };
    // SAFETY: as above; r is distinct from t and p.
    let result = unsafe { &mut *base.add(r) };
    let mut slots = EvalSlots { scan: Some(scan), inner: None, outer };
    execexpr::exec_project(state, &mut slots, result, mcx)?;
    Ok(result_id)
}

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
fn exec_insert<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let onconflict = mt.plan.onConflictAction;
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);

    if mt.result_relkind == types_rel::RELKIND_VIEW {
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
        if mt.canSetTag {
            estate.es_processed += 1;
        }
        return Ok(Some(slot_id));
    }

    if mt.trigdesc.as_ref().is_some_and(|td| td.trig_insert_before_row) {
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_INSERT,
            types_trigger::TRIGGER_EVENT_INSERT,
            None,
            Some(slot_id),
        )? {
            return Ok(None);
        }
    }

    // ExecPrepareTupleRouting: partitioned targets route to a leaf; slots are
    // shared unconverted (attno-remapped children are loud in the router).
    let leaf_idx = {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let target = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        if target.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            if onconflict != 0 {
                panic!("ExecInsert: ON CONFLICT on a partitioned table not ported");
            }
            let slot = &mut es_tupleTable[slot_id.0 as usize];
            let router = match mt.router.as_mut() {
                Some(r) => r,
                None => {
                    mt.router =
                        Some(execpartition::PartitionTupleRouting::new(mcx, target)?);
                    mt.router.as_mut().unwrap()
                }
            };
            let idx = router.find_partition(slot)?;
            while mt.leaf_indexes.len() <= idx {
                mt.leaf_indexes.push(None);
                mt.leaf_checks.push(None);
                mt.leaf_virtual_nn.push(None);
            }
            Some(idx)
        } else {
            None
        }
    };

    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let slot = &mut es_tupleTable[slot_id.0 as usize];
        let (rel, indexes, check_exprs, virtual_nn_exprs) = match leaf_idx {
            Some(idx) => (
                mt.router.as_ref().unwrap().leaf_rel(idx),
                &mut mt.leaf_indexes[idx],
                &mut mt.leaf_checks[idx],
                &mut mt.leaf_virtual_nn[idx],
            ),
            None => (
                es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut mt.indexes,
                &mut mt.check_exprs,
                &mut mt.virtual_nn_exprs,
            ),
        };
        if leaf_idx.is_some() && rel.rd_hastriggers {
            panic!("ExecInsert: row triggers on a routed-into partition not ported");
        }

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            exec_compute_stored_generated(mcx, &mut mt.generated_exprs, rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        if rel.rd_rel.relhasindex && indexes.is_none() {
            *indexes = Some(execindexing::ExecOpenIndices(mcx, rel, onconflict != 0)?);
        }

        if !mt.wco_exprs.is_empty() {
            if leaf_idx.is_some() {
                panic!("ExecInsert: WCOs on a routed partition (leaf attr map) not ported");
            }
            let wco_kind = if mt.operation == CmdType::CMD_UPDATE {
                WCOKind::WCO_RLS_UPDATE_CHECK
            } else {
                WCOKind::WCO_RLS_INSERT_CHECK
            };
            exec_with_check_options(&mut mt.wco_exprs, wco_kind, slot)?;
        }

        exec_constraints(mcx, check_exprs, virtual_nn_exprs, rel, slot)?;
    }

    let num_indices = mt.indexes.as_ref().map_or(0, |x| x.num_indices());
    if onconflict != 0 && num_indices > 0 {
        let existing_id = mt.on_conflict.as_ref().expect("on_conflict state").existing_slot;
        // vlock:
        loop {
            let mut conflict_tid = ItemPointerData::default();
            ItemPointerSetInvalid(&mut conflict_tid);
            let mut invalid_tid = ItemPointerData::default();
            ItemPointerSetInvalid(&mut invalid_tid);

            let pre_ok = {
                let oc = mt.on_conflict.as_ref().expect("on_conflict state");
                let indexes = mt.indexes.as_mut().expect("indexes opened");
                let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
                let rel = es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                let (s, e) = (slot_id.0 as usize, existing_id.0 as usize);
                assert!(s != e && s < es_tupleTable.len() && e < es_tupleTable.len());
                let base = es_tupleTable.as_mut_ptr();
                // SAFETY: distinct in-bounds indices of one live slice.
                let (slot, existing) = unsafe { (&mut *base.add(s), &mut *base.add(e)) };
                execindexing::ExecCheckIndexConstraints(
                    mcx,
                    mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    existing,
                    &invalid_tid,
                    &oc.arbiters,
                    &mut conflict_tid,
                )?
            };

            if !pre_ok {
                // Committed conflict tuple found.
                if onconflict == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32 {
                    match exec_on_conflict_update(mt, estate, conflict_tid, slot_id, epq_eval)? {
                        OnConflictOutcome::Done(rslot) => return Ok(rslot),
                        OnConflictOutcome::Retry => continue,
                    }
                }
                exec_check_tid_visible(mt, estate, &conflict_tid)?;
                return Ok(None);
            }

            let xid = xact::GetCurrentTransactionId()?;
            let spec_token = lmgr::SpeculativeInsertionLockAcquire(xid)?;
            let mut spec_conflict = false;
            {
                let oc = mt.on_conflict.as_ref().expect("on_conflict state");
                let indexes = mt.indexes.as_mut().expect("indexes opened");
                let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
                let rel = es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                let slot = &mut es_tupleTable[slot_id.0 as usize];
                tableam::table_tuple_insert_speculative(
                    mcx, rel, slot, output_cid, 0, None, spec_token,
                )?;
                recheck_indexes = execindexing::ExecInsertIndexTuples(
                    mcx,
                    mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    true,
                    Some(&mut spec_conflict),
                    &oc.arbiters,
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

            if spec_conflict {
                continue;
            }
            break;
        }
    } else {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let slot = &mut es_tupleTable[slot_id.0 as usize];
        let (rel, indexes) = match leaf_idx {
            Some(idx) => (
                mt.router.as_ref().unwrap().leaf_rel(idx),
                &mut mt.leaf_indexes[idx],
            ),
            None => (
                es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut mt.indexes,
            ),
        };

        tableam::table_tuple_insert(mcx, rel, slot, output_cid, 0, None)?;

        if let Some(indexes) = indexes.as_mut() {
            if indexes.num_indices() > 0 {
                recheck_indexes = execindexing::ExecInsertIndexTuples(
                    mcx,
                    mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                )?;
            }
        }
    }

    if let Some(td) = &mt.trigdesc {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &es_tupleTable[slot_id.0 as usize];
        ::trigger::ExecARInsertTriggers(mcx, rel, td, slot.base().tts_tid, &recheck_indexes)?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(Some(slot_id))
}

// ExecOnConflictUpdate (nodeModifyTable.c): lock the conflict tuple, verify
// visibility, apply the DO UPDATE WHERE qual and SET projection, then run the
// plain UPDATE path against the locked tuple.
fn exec_on_conflict_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    conflict_tid: ItemPointerData,
    excluded_id: ExecSlotId,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<OnConflictOutcome> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let (existing_id, setvals_id, proj_id) = {
        let oc = mt.on_conflict.as_ref().expect("on_conflict state");
        (
            oc.existing_slot,
            oc.setvals_slot.expect("DO UPDATE state"),
            oc.proj_slot.expect("DO UPDATE state"),
        )
    };

    let mut tmfd = TM_FailureData::default();
    // ExecUpdateLockMode: the UPDATE path always takes LockTupleExclusive
    // (NoKeyExclusive needs the unchanged-key-columns analysis, unported).
    let lock_result = {
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        tableam::table_tuple_lock(
            mcx,
            rel,
            &conflict_tid,
            snapshot,
            &mut es_tupleTable[existing_id.0 as usize],
            output_cid,
            LockTupleMode::LockTupleExclusive,
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
                let rel = es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
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

    exec_check_tuple_visible(mt, estate, existing_id)?;

    // EXCLUDED reads through INNER_VAR (setrefs), the existing tuple through
    // scan Vars; evaluate the WHERE qual then the SET projection that way.
    {
        let oc = mt.on_conflict.as_mut().expect("on_conflict state");
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
        if !execexpr::exec_qual(oc.where_clause.as_deref_mut(), &mut slots)? {
            exectuples::exec_clear_tuple(slots.scan.take().expect("scan slot"), mcx);
            return Ok(OnConflictOutcome::Done(None));
        }

        if !mt.wco_exprs.is_empty() {
            let scan = slots.scan.take().expect("scan slot");
            exec_with_check_options(&mut mt.wco_exprs, WCOKind::WCO_RLS_CONFLICT_CHECK, scan)?;
            slots.scan = Some(scan);
        }

        let set_proj = oc.set_proj.as_deref_mut().expect("DO UPDATE projection");
        execexpr::exec_project(set_proj, &mut slots, setvals, mcx)?;
    }

    // Merge SET values over the existing tuple into the projected new tuple.
    {
        let oc = mt.on_conflict.as_ref().expect("on_conflict state");
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
            for (i, &attno) in oc.set_attnos.iter().enumerate() {
                pb.tts_values[attno as usize - 1] = vb.tts_values[i];
                pb.tts_isnull[attno as usize - 1] = vb.tts_isnull[i];
            }
        }
        exectuples::exec_store_virtual_tuple(proj);
    }

    let mut tupleid = conflict_tid;
    let modified = exec_update(mt, estate, &mut tupleid, proj_id, epq_eval)?;
    clear_slot(estate, existing_id);
    Ok(OnConflictOutcome::Done(if modified { Some(proj_id) } else { None }))
}

// ExecCheckTIDVisible (nodeModifyTable.c): under xact-snapshot isolation the
// DO NOTHING skip must not be based on a tuple invisible to our snapshot.
fn exec_check_tid_visible<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tid: &ItemPointerData,
) -> PgResult<()> {
    if !xact::IsolationUsesXactSnapshot() {
        return Ok(());
    }
    let existing_id = mt.on_conflict.as_ref().expect("on_conflict state").existing_slot;
    let found = {
        let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = &mut *estate;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        tableam::table_tuple_fetch_row_version(
            *es_query_cxt,
            rel,
            tid,
            &mt.snapshot_any,
            &mut es_tupleTable[existing_id.0 as usize],
        )?
    };
    assert!(found, "failed to fetch conflicting tuple for ON CONFLICT");
    exec_check_tuple_visible(mt, estate, existing_id)?;
    clear_slot(estate, existing_id);
    Ok(())
}

// ExecCheckTupleVisible (nodeModifyTable.c).
fn exec_check_tuple_visible<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<()> {
    if !xact::IsolationUsesXactSnapshot() {
        return Ok(());
    }
    let visible = {
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
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
// every kind (ExecQual semantics); VIEW_CHECK is loud at init.
fn exec_with_check_options<'mcx>(
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
        WCOKind::WCO_VIEW_CHECK => unreachable!("loud at init"),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE))
}

// ExecConstraints (execMain.c): NOT NULL + CHECK arms live.
fn exec_constraints<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    check_exprs: &mut Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>,
    virtual_nn_exprs: &mut Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    if let Some(constr) = rel.rd_att.constr.as_deref() {
        if constr.has_not_null {
            exec_not_null_constraints(mcx, rel, slot)?;
            if constr.has_generated_virtual {
                if let Some(i) = exec_rel_gen_virtual_notnull(mcx, virtual_nn_exprs, rel, slot)? {
                    return Err(not_null_violation(mcx, rel, slot, i));
                }
            }
        }
        if constr.num_check > 0 {
            if let Some(failed) = exec_rel_check(mcx, check_exprs, rel, slot)? {
                return Err(check_violation(mcx, rel, slot, failed));
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
            let state = execexpr::exec_init_expr(mcx, Some(node), execexpr::ParamBind::NONE)?
                .expect("check constraint expr");
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
) -> PgResult<()> {
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attgenerated == VIRTUAL_GEN {
            continue;
        }
        if att.attnotnull && exectuples::slot_attisnull(slot, i as i32 + 1) {
            return Err(not_null_violation(mcx, rel, slot, i));
        }
    }
    Ok(())
}

const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;

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
            panic!(
                "expand_generated_columns_in_expr (rewriteHandler.c): whole-row Var \
                 over a virtual-generated relation unported"
            );
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

// ExecBuildSlotValueDescription (execMain.c), table-SELECT-permission arm
// (single-superuser boot: column-ACL filtering and RLS are unreachable).
fn slot_value_description<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<String> {
    const MAX_FIELD_LEN: usize = 64;
    exectuples::slot_getallattrs(slot);
    let mut buf = String::from("(");
    let mut write_comma = false;
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attisdropped {
            continue;
        }
        if write_comma {
            buf.push_str(", ");
        }
        write_comma = true;
        if att.attgenerated == VIRTUAL_GEN {
            buf.push_str("virtual");
            continue;
        }
        let base = slot.base();
        if base.tts_isnull[i] {
            buf.push_str("null");
            continue;
        }
        let value = base.tts_values[i];
        let (foutoid, _) = lsyscache::typ::getTypeOutputInfo(att.atttypid)?;
        let mut finfo = fmgr_core::fmgr_info(foutoid)?;
        let out = fmgr_core::function_call1_coll_in(&mut finfo, 0, mcx, value)?;
        // SAFETY: output fns return a NUL-terminated cstring datum.
        let s = unsafe {
            core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char)
        }
        .to_bytes();
        let s = core::str::from_utf8(s).expect("type output is UTF-8");
        if s.len() <= MAX_FIELD_LEN {
            buf.push_str(s);
        } else {
            let mut end = MAX_FIELD_LEN;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            buf.push_str(&s[..end]);
            buf.push_str("...");
        }
    }
    buf.push(')');
    Ok(buf)
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

#[cold]
#[inline(never)]
fn not_null_violation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    attidx: usize,
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
    if let Ok(desc) = slot_value_description(mcx, rel, slot) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    e.column_name = Some(col);
    Box::new(e)
}

#[cold]
#[inline(never)]
fn check_violation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    failed: usize,
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
    if let Ok(desc) = slot_value_description(mcx, rel, slot) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    Box::new(e)
}

#[cold]
#[inline(never)]
fn plan_output_mismatch(detail: &'static str) -> Box<PgError> {
    Box::new(
        PgError::error("table row type and query-specified row type do not match")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_detail(detail),
    )
}

mcx::forget_safe_nodrop!(NewColSrc);

// Exempt: indexes/snapshot_any/project_returning/on_conflict/check_exprs/
// trigdesc/generated_exprs/router/leaf_indexes/leaf_checks/index_eval_cx/merge
// (and each CheckExpr's/GeneratedExpr's state) are
// released in exec_end_modify_table; CmdType is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<CmdType>());
mcx::forget_safe_struct!(
    CheckExpr<'_> { name; state },
    GeneratedExpr<'_> { attnum; state },
    VirtualNnExpr<'_> { attnum; state },
    WcoExpr<'_> { kind, relname, polname; state },
    ModifyTableState<'_> { plan, canSetTag, mt_done, fireBSTriggers, result_relkind, result_rti,
        ri_newTupleSlot, ri_oldTupleSlot, ri_ReturningSlot,
        ri_projectNewInfoValid, ri_RowIdAttNo, update_cols, returning_slot;
        operation, indexes, snapshot_any, project_returning, on_conflict,
        check_exprs, trigdesc, trig_fmgr, trig_old_slot, generated_exprs,
        virtual_nn_exprs, router, leaf_indexes, leaf_checks, leaf_virtual_nn,
        index_eval_cx, wco_exprs, merge },
);
