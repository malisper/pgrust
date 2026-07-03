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
    // ri_GeneratedExprsI/U collapsed to one set: the UPDATE updatedCols skip
    // is perf-only (values are immutable functions of non-generated columns).
    generated_exprs: Option<mcx::PgVec<'mcx, GeneratedExpr<'mcx>>>,
    // Partitioned-target INSERT routing (execPartition.c); per-leaf insert
    // state is indexed by the router's leaf index.
    router: Option<execpartition::PartitionTupleRouting<'mcx>>,
    leaf_indexes: Vec<Option<execindexing::ResultRelIndexState<'mcx>>>,
    leaf_checks: Vec<Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>>,
}

struct GeneratedExpr<'mcx> {
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
        CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE
    ) {
        panic!(
            "ExecInitModifyTable (nodeModifyTable.c): {:?} arm not ported",
            node.operation
        );
    }
    if !node.mergeActionLists.is_nil() || !node.fdwPrivLists.is_nil() {
        panic!("ExecInitModifyTable (nodeModifyTable.c): MERGE/FDW lists not ported");
    }
    assert_eq!(node.resultRelations.len(), 1);
    debug_assert!(node.rootRelation == 0 && node.rowMarks.is_nil());
    let rti = node.resultRelations.nth(0) as u32;
    debug_assert!(estate.es_unpruned_relids.is_member(rti as i32));

    estate.exec_init_result_relation(rti)?;
    let trigdesc = {
        let rel = estate.es_relations[(rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        check_valid_result_rel(rel, node.operation);
        if rel.rd_hastriggers {
            let td = relcache::RelationGetTriggerDesc(rel.rd_id)?;
            if let Some(td) = &td {
                let unported = td.trig_insert_before_row
                    || td.trig_insert_instead_row
                    || td.trig_update_before_row
                    || td.trig_update_instead_row
                    || td.trig_delete_before_row
                    || td.trig_delete_instead_row
                    || td.trig_insert_before_statement
                    || td.trig_insert_after_statement
                    || td.trig_update_before_statement
                    || td.trig_update_after_statement
                    || td.trig_delete_before_statement
                    || td.trig_delete_after_statement;
                if unported {
                    panic!(
                        "ExecInitModifyTable (nodeModifyTable.c): BEFORE/INSTEAD/                         statement triggers unported (AFTER ROW RI lane only)"
                    );
                }
            }
            td
        } else {
            None
        }
    };

    // The UPDATE/DELETE row identity: the plain-relation leg carries a junk
    // ctid attribute in the subplan targetlist (wholerow legs are the
    // FDW/view lanes, loud at CheckValidResultRel).
    let mut rowid_attno: i16 = 0;
    if matches!(node.operation, CmdType::CMD_UPDATE | CmdType::CMD_DELETE) {
        let subplan = node
            .plan
            .lefttree
            .expect("ModifyTable has a subplan")
            .as_plan()
            .expect("plan node");
        rowid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "ctid");
        assert!(rowid_attno > 0, "could not find junk ctid column");
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

    // fireBSTriggers/ExecSetupTransitionCaptureState: the trimmed relcache
    // entry carries no trigger descriptor, so statement triggers are
    // undetectable until pg_trigger lands (none exist without CREATE TRIGGER).
    Ok(ModifyTableState {
        plan: node,
        operation: node.operation,
        canSetTag: node.canSetTag,
        mt_done: false,
        result_rti: rti,
        ri_newTupleSlot: None,
        ri_oldTupleSlot: None,
        ri_ReturningSlot: None,
        ri_projectNewInfoValid: false,
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
        generated_exprs: None,
        router: None,
        leaf_indexes: Vec::new(),
        leaf_checks: Vec::new(),
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
fn check_valid_result_rel(rel: &Relation<'_>, operation: CmdType) {
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        if operation != CmdType::CMD_INSERT {
            panic!(
                "ExecInitModifyTable: {operation:?} on a partitioned table                  (inherited result relations) not ported"
            );
        }
        return;
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
            CmdType::CMD_DELETE => {
                let mut tupleid = fetch_row_id(mt, estate, plan_slot);
                let modified = exec_delete(mt, estate, &mut tupleid, &mut epq_eval)?;
                if modified && mt.project_returning.is_some() {
                    let old_slot = exec_delete_fetch_old(mt, estate, &tupleid)?;
                    return Ok(Some(exec_process_returning(mt, estate, old_slot, plan_slot)?));
                }
            }
            other => panic!("ExecModifyTable (nodeModifyTable.c): {other:?} arm not ported"),
        }
    }

    debug_assert!(estate.es_insert_pending_result_relations.is_empty());
    mt.mt_done = true;
    Ok(None)
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

/// `ExecEndModifyTable` node-local half; the caller ends the subplan.
pub fn exec_end_modify_table(mt: &mut ModifyTableState<'_>) {
    if let Some(indexes) = mt.indexes.take() {
        execindexing::ExecCloseIndices(indexes).expect("ExecCloseIndices");
    }
    mt.snapshot_any = None;
    mt.project_returning = None;
    mt.on_conflict = None;
    mt.check_exprs = None;
    mt.trigdesc = None;
    mt.generated_exprs = None;
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
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_NextValueExpr => {
            node.as_variant::<types_nodes::primnodes::NextValueExpr>().unwrap().typeId
        }
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
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
            exec_constraints(mcx, &mut mt.check_exprs, rel, slot)?;

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
    if let Some(indexes) = mt.indexes.as_mut() {
        if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
            if update_indexes == TU_UpdateIndexes::TU_Summarizing {
                panic!(
                    "ExecUpdateEpilogue (nodeModifyTable.c): onlySummarizing \
                     index maintenance (BRIN lane) not ported"
                );
            }
            execindexing::ExecInsertIndexTuples(
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
        ::trigger::ExecARUpdateTriggers(mcx, rel, td, *tupleid, slot.base().tts_tid)?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(true)
}

// ExecDelete + ExecDeletePrologue/Act/Epilogue (nodeModifyTable.c), plain-heap
// arm; concurrent TM_Updated runs the EPQ recheck (ldelete loop).
fn exec_delete<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    epq_eval: &mut impl FnMut(&mut EStateData<'mcx>, ExecSlotId) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<bool> {
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();

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
            }
            Some(idx)
        } else {
            None
        }
    };

    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let slot = &mut es_tupleTable[slot_id.0 as usize];
        let (rel, indexes, check_exprs) = match leaf_idx {
            Some(idx) => (
                mt.router.as_ref().unwrap().leaf_rel(idx),
                &mut mt.leaf_indexes[idx],
                &mut mt.leaf_checks[idx],
            ),
            None => (
                es_relations[(mt.result_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut mt.indexes,
                &mut mt.check_exprs,
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

        exec_constraints(mcx, check_exprs, rel, slot)?;
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
                execindexing::ExecInsertIndexTuples(
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
                execindexing::ExecInsertIndexTuples(
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
        ::trigger::ExecARInsertTriggers(mcx, rel, td, slot.base().tts_tid)?;
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
fn exec_compute_stored_generated<'mcx>(
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
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    if let Some(constr) = rel.rd_att.constr.as_deref() {
        if constr.has_generated_virtual {
            panic!("unported: virtual generated columns");
        }
        if constr.has_not_null {
            exec_not_null_constraints(mcx, rel, slot)?;
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
            let node = readfuncs::stringToNode(mcx, ccbin.as_str())?;
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
        if att.attnotnull && exectuples::slot_attisnull(slot, i as i32 + 1) {
            return Err(not_null_violation(mcx, rel, slot, i));
        }
    }
    Ok(())
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
// trigdesc/generated_exprs/router/leaf_indexes/leaf_checks/index_eval_cx (and
// each CheckExpr's/GeneratedExpr's state) are
// released in exec_end_modify_table; CmdType is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<CmdType>());
mcx::forget_safe_struct!(
    CheckExpr<'_> { name; state },
    GeneratedExpr<'_> { attnum; state },
    ModifyTableState<'_> { plan, canSetTag, mt_done, result_rti,
        ri_newTupleSlot, ri_oldTupleSlot, ri_ReturningSlot,
        ri_projectNewInfoValid, ri_RowIdAttNo, update_cols, returning_slot;
        operation, indexes, snapshot_any, project_returning, on_conflict,
        check_exprs, trigdesc, generated_exprs, router, leaf_indexes, leaf_checks,
        index_eval_cx },
);
