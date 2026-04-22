use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::backend::access::index::unique::{UniqueProbeConflict, probe_unique_conflict};
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::executor::{ExecError, ExecutorContext, Expr, Value, eval_expr};
use crate::backend::parser::{
    BoundAssignmentTarget, BoundIndexRelation, BoundInsertStatement, BoundOnConflictAction,
    BoundOnConflictClause,
};
use crate::include::access::htup::HeapTuple;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::execnodes::TupleSlot;

use super::tablecmds::{
    WriteUpdatedRowResult, apply_assignment_target, build_index_insert_context,
    index_key_values_for_row, insert_index_entry_for_row, rollback_inserted_row,
    slot_toast_context, write_insert_heap_row, write_updated_row,
};

enum EvaluatedConflictAction {
    Updated(Vec<Value>),
    Skipped,
}

enum ConflictActionResult {
    Updated(Vec<Value>),
    Skipped,
    Retry,
}

struct ArbiterConflict {
    tid: ItemPointerData,
    tuple: HeapTuple,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArbiterKey {
    index_name: String,
    index_relation_oid: u32,
    key_values: Vec<Value>,
}

const ON_CONFLICT_DO_UPDATE_CARDINALITY_MESSAGE: &str =
    "ON CONFLICT DO UPDATE command cannot affect row a second time";
const ON_CONFLICT_DO_UPDATE_CARDINALITY_HINT: &str = "Ensure that no rows proposed for insertion within the same command have duplicate constrained values.";

fn cardinality_violation() -> ExecError {
    ExecError::CardinalityViolation {
        message: ON_CONFLICT_DO_UPDATE_CARDINALITY_MESSAGE.into(),
        hint: Some(ON_CONFLICT_DO_UPDATE_CARDINALITY_HINT.into()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SimulatedRowId {
    Existing(ItemPointerData),
    Inserted(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulatedRowOrigin {
    Existing,
    Inserted,
}

#[derive(Debug, Clone)]
struct SimulatedRowState {
    current_values: Vec<Value>,
    arbiter_keys: Vec<ArbiterKey>,
    origin: SimulatedRowOrigin,
}

fn arbiter_keys_for_row(
    arbiter_indexes: &[&BoundIndexRelation],
    desc: &crate::backend::executor::RelationDesc,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<ArbiterKey>, ExecError> {
    let mut keys = Vec::with_capacity(arbiter_indexes.len());
    for index in arbiter_indexes {
        let mut key_values = index_key_values_for_row(index, desc, values, ctx)?;
        if !index.index_meta.indnullsnotdistinct
            && key_values.iter().any(|value| matches!(value, Value::Null))
        {
            continue;
        }
        Value::materialize_all(&mut key_values);
        keys.push(ArbiterKey {
            index_name: index.name.clone(),
            index_relation_oid: index.relation_oid,
            key_values,
        });
    }
    Ok(keys)
}

fn eval_bool_qual(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn with_conflict_bindings<T, F>(
    current_values: &[Value],
    excluded_values: &[Value],
    ctx: &mut ExecutorContext,
    f: F,
) -> Result<T, ExecError>
where
    F: FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
{
    let saved_outer_tuple = ctx
        .expr_bindings
        .outer_tuple
        .replace(current_values.to_vec());
    let saved_outer_system_bindings = std::mem::take(&mut ctx.expr_bindings.outer_system_bindings);
    let saved_inner_tuple = ctx
        .expr_bindings
        .inner_tuple
        .replace(excluded_values.to_vec());
    let saved_inner_system_bindings = std::mem::take(&mut ctx.expr_bindings.inner_system_bindings);
    let result = f(ctx);
    ctx.expr_bindings.outer_tuple = saved_outer_tuple;
    ctx.expr_bindings.outer_system_bindings = saved_outer_system_bindings;
    ctx.expr_bindings.inner_tuple = saved_inner_tuple;
    ctx.expr_bindings.inner_system_bindings = saved_inner_system_bindings;
    result
}

fn decode_tuple_values(
    stmt: &BoundInsertStatement,
    desc: &Rc<crate::backend::executor::RelationDesc>,
    attr_descs: &Rc<[crate::include::access::htup::AttributeDesc]>,
    tid: ItemPointerData,
    tuple: HeapTuple,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut slot = TupleSlot::from_heap_tuple(Rc::clone(desc), Rc::clone(attr_descs), tid, tuple);
    slot.toast = slot_toast_context(stmt.toast, ctx);
    slot.into_values()
}

fn probe_arbiter_conflict(
    stmt: &BoundInsertStatement,
    arbiter_indexes: &[&BoundIndexRelation],
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Option<ArbiterConflict>, ExecError> {
    for index in arbiter_indexes {
        let key_values = index_key_values_for_row(index, &stmt.desc, values, ctx)?;
        let insert_ctx = build_index_insert_context(
            stmt.rel,
            &stmt.desc,
            index,
            key_values,
            ItemPointerData::default(),
            ctx,
        );
        if let Some(UniqueProbeConflict { tid, tuple }) =
            probe_unique_conflict(&insert_ctx, &insert_ctx.values)?
        {
            return Ok(Some(ArbiterConflict { tid, tuple }));
        }
    }
    Ok(None)
}

fn run_conflict_update(
    stmt: &BoundInsertStatement,
    assignments: &[crate::backend::parser::BoundAssignment],
    predicate: Option<&Expr>,
    excluded_values: &[Value],
    conflict_tid: ItemPointerData,
    conflict_tuple: HeapTuple,
    desc: &Rc<crate::backend::executor::RelationDesc>,
    attr_descs: &Rc<[crate::include::access::htup::AttributeDesc]>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<ConflictActionResult, ExecError> {
    if conflict_tuple.header.xmin == xid && conflict_tuple.header.cid_or_xvac == cid {
        return Err(cardinality_violation());
    }

    let current_old_values =
        decode_tuple_values(stmt, desc, attr_descs, conflict_tid, conflict_tuple, ctx)?;
    let new_values = match eval_conflict_update_values(
        &stmt.desc,
        assignments,
        predicate,
        &current_old_values,
        excluded_values,
        ctx,
    )? {
        EvaluatedConflictAction::Updated(new_values) => new_values,
        EvaluatedConflictAction::Skipped => return Ok(ConflictActionResult::Skipped),
    };

    let write_result = write_updated_row(
        &stmt.relation_name,
        stmt.rel,
        stmt.toast,
        stmt.toast_index.as_ref(),
        &stmt.desc,
        &stmt.relation_constraints,
        &[],
        &stmt.referenced_by_foreign_keys,
        &stmt.indexes,
        conflict_tid,
        &current_old_values,
        &new_values,
        ctx,
        xid,
        cid,
        None,
    )?;
    match write_result {
        WriteUpdatedRowResult::Updated(_new_tid) => Ok(ConflictActionResult::Updated(new_values)),
        WriteUpdatedRowResult::TupleUpdated(_new_tid) => Ok(ConflictActionResult::Retry),
        WriteUpdatedRowResult::AlreadyModified => Ok(ConflictActionResult::Retry),
    }
}

fn eval_conflict_update_values(
    desc: &crate::backend::executor::RelationDesc,
    assignments: &[crate::backend::parser::BoundAssignment],
    predicate: Option<&Expr>,
    current_values: &[Value],
    excluded_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<EvaluatedConflictAction, ExecError> {
    let mut eval_slot = TupleSlot::virtual_row(current_values.to_vec());
    let mut new_values = current_values.to_vec();
    let result = with_conflict_bindings(current_values, excluded_values, ctx, |ctx| {
        if let Some(predicate) = predicate {
            if !eval_bool_qual(predicate, &mut eval_slot, ctx)? {
                return Ok(EvaluatedConflictAction::Skipped);
            }
        }
        for assignment in assignments {
            let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
            apply_assignment_target(
                desc,
                &mut new_values,
                &BoundAssignmentTarget {
                    column_index: assignment.column_index,
                    subscripts: assignment.subscripts.clone(),
                    field_path: assignment.field_path.clone(),
                    target_sql_type: assignment.target_sql_type,
                },
                value,
                &mut eval_slot,
                ctx,
            )?;
        }
        Value::materialize_all(&mut new_values);
        Ok(EvaluatedConflictAction::Updated(new_values.clone()))
    })?;
    Ok(result)
}

fn record_simulated_row_state(
    simulated_rows: &mut HashMap<SimulatedRowId, SimulatedRowState>,
    arbiter_row_map: &mut HashMap<ArbiterKey, SimulatedRowId>,
    row_id: SimulatedRowId,
    state: SimulatedRowState,
) -> Result<(), ExecError> {
    if let Some(previous) = simulated_rows.remove(&row_id) {
        for key in previous.arbiter_keys {
            arbiter_row_map.remove(&key);
        }
    }
    for key in &state.arbiter_keys {
        if let Some(existing_row_id) = arbiter_row_map.get(key) {
            if *existing_row_id != row_id {
                return Err(ExecError::UniqueViolation {
                    constraint: key.index_name.clone(),
                    detail: None,
                });
            }
        }
    }
    for key in &state.arbiter_keys {
        arbiter_row_map.insert(key.clone(), row_id);
    }
    simulated_rows.insert(row_id, state);
    Ok(())
}

fn preflight_on_conflict_updates(
    stmt: &BoundInsertStatement,
    arbiter_indexes: &[&BoundIndexRelation],
    assignments: &[crate::backend::parser::BoundAssignment],
    predicate: Option<&Expr>,
    rows: &[Vec<Value>],
    desc: &Rc<crate::backend::executor::RelationDesc>,
    attr_descs: &Rc<[crate::include::access::htup::AttributeDesc]>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let mut simulated_rows = HashMap::<SimulatedRowId, SimulatedRowState>::new();
    let mut arbiter_row_map = HashMap::<ArbiterKey, SimulatedRowId>::new();
    let mut next_inserted_row_id = 0usize;

    for values in rows {
        let proposed_arbiter_keys = arbiter_keys_for_row(arbiter_indexes, &stmt.desc, values, ctx)?;
        if proposed_arbiter_keys
            .iter()
            .any(|key| arbiter_row_map.contains_key(key))
        {
            return Err(cardinality_violation());
        }

        let storage_conflict = probe_arbiter_conflict(stmt, arbiter_indexes, values, ctx)?;
        let stale_simulated_storage_conflict = storage_conflict
            .as_ref()
            .map(|conflict| SimulatedRowId::Existing(conflict.tid))
            .and_then(|row_id| simulated_rows.get(&row_id).map(|state| (row_id, state)));

        if let Some((row_id, state)) = stale_simulated_storage_conflict {
            debug_assert!(matches!(state.origin, SimulatedRowOrigin::Existing));
            let mut inserted_values = values.clone();
            Value::materialize_all(&mut inserted_values);
            record_simulated_row_state(
                &mut simulated_rows,
                &mut arbiter_row_map,
                SimulatedRowId::Inserted(next_inserted_row_id),
                SimulatedRowState {
                    current_values: inserted_values,
                    arbiter_keys: proposed_arbiter_keys,
                    origin: SimulatedRowOrigin::Inserted,
                },
            )?;
            next_inserted_row_id += 1;
            debug_assert!(matches!(row_id, SimulatedRowId::Existing(_)));
            continue;
        }

        if let Some(conflict) = storage_conflict {
            let current_values =
                decode_tuple_values(stmt, desc, attr_descs, conflict.tid, conflict.tuple, ctx)?;
            match eval_conflict_update_values(
                &stmt.desc,
                assignments,
                predicate,
                &current_values,
                values,
                ctx,
            )? {
                EvaluatedConflictAction::Updated(updated_values) => {
                    let updated_arbiter_keys =
                        arbiter_keys_for_row(arbiter_indexes, &stmt.desc, &updated_values, ctx)?;
                    record_simulated_row_state(
                        &mut simulated_rows,
                        &mut arbiter_row_map,
                        SimulatedRowId::Existing(conflict.tid),
                        SimulatedRowState {
                            current_values: updated_values,
                            arbiter_keys: updated_arbiter_keys,
                            origin: SimulatedRowOrigin::Existing,
                        },
                    )?;
                }
                EvaluatedConflictAction::Skipped => {}
            }
            continue;
        }

        let mut inserted_values = values.clone();
        Value::materialize_all(&mut inserted_values);
        record_simulated_row_state(
            &mut simulated_rows,
            &mut arbiter_row_map,
            SimulatedRowId::Inserted(next_inserted_row_id),
            SimulatedRowState {
                current_values: inserted_values,
                arbiter_keys: proposed_arbiter_keys,
                origin: SimulatedRowOrigin::Inserted,
            },
        )?;
        next_inserted_row_id += 1;
    }

    Ok(())
}

pub(crate) fn execute_insert_on_conflict_rows(
    stmt: &BoundInsertStatement,
    on_conflict: &BoundOnConflictClause,
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Vec<Vec<Value>>, ExecError> {
    let desc = Rc::new(stmt.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let arbiter_index_oids = on_conflict
        .arbiter_indexes
        .iter()
        .map(|index| index.relation_oid)
        .collect::<HashSet<_>>();
    let arbiter_indexes = on_conflict
        .arbiter_indexes
        .iter()
        .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
        .collect::<Vec<_>>();
    let non_arbiter_indexes = stmt
        .indexes
        .iter()
        .filter(|index| {
            index.index_meta.indisvalid
                && index.index_meta.indisready
                && !arbiter_index_oids.contains(&index.relation_oid)
        })
        .collect::<Vec<_>>();
    let mut affected_rows = Vec::new();

    if let BoundOnConflictAction::Update {
        assignments,
        predicate,
    } = &on_conflict.action
    {
        preflight_on_conflict_updates(
            stmt,
            &arbiter_indexes,
            assignments,
            predicate.as_ref(),
            rows,
            &desc,
            &attr_descs,
            ctx,
        )?;
    }

    for values in rows {
        loop {
            ctx.check_for_interrupts()?;
            if let Some(conflict) = probe_arbiter_conflict(stmt, &arbiter_indexes, values, ctx)? {
                match &on_conflict.action {
                    BoundOnConflictAction::Nothing => break,
                    BoundOnConflictAction::Update {
                        assignments,
                        predicate,
                    } => match run_conflict_update(
                        stmt,
                        assignments,
                        predicate.as_ref(),
                        values,
                        conflict.tid,
                        conflict.tuple,
                        &desc,
                        &attr_descs,
                        ctx,
                        xid,
                        cid,
                    )? {
                        ConflictActionResult::Updated(updated_values) => {
                            affected_rows.push(updated_values);
                            break;
                        }
                        ConflictActionResult::Skipped => break,
                        ConflictActionResult::Retry => continue,
                    },
                }
            }

            let heap_tid = write_insert_heap_row(
                &stmt.relation_name,
                stmt.rel,
                stmt.toast,
                stmt.toast_index.as_ref(),
                &stmt.desc,
                &stmt.relation_constraints,
                &stmt.rls_write_checks,
                values,
                ctx,
                xid,
                cid,
            )?;

            let mut retry_conflict = false;
            for index in &arbiter_indexes {
                match insert_index_entry_for_row(stmt.rel, &stmt.desc, index, values, heap_tid, ctx)
                {
                    Ok(()) => {}
                    Err(ExecError::UniqueViolation {
                        constraint,
                        detail: _,
                    }) if constraint.eq_ignore_ascii_case(&index.name) => {
                        rollback_inserted_row(
                            stmt.rel, stmt.toast, &stmt.desc, heap_tid, ctx, xid,
                        )?;
                        retry_conflict = true;
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }
            if retry_conflict {
                continue;
            }

            for index in &non_arbiter_indexes {
                insert_index_entry_for_row(stmt.rel, &stmt.desc, index, values, heap_tid, ctx)?;
            }
            let mut inserted_values = values.to_vec();
            Value::materialize_all(&mut inserted_values);
            affected_rows.push(inserted_values);
            break;
        }
    }

    Ok(affected_rows)
}
