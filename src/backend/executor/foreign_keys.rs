use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;

use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::{CommandId, Snapshot};
use crate::backend::executor::exec_tuples::CompiledTupleDecoder;
use crate::backend::parser::{
    BoundForeignKeyConstraint, BoundReferencedByForeignKey, ForeignKeyMatchType,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{SlotKind, ToastRelationRef, TupleSlot};

use super::{ExecError, ExecutorContext, compare_order_values};

fn maybe_defer_constraint(
    ctx: &ExecutorContext,
    constraint_oid: u32,
    deferrable: bool,
    initially_deferred: bool,
) -> bool {
    if !deferrable || !initially_deferred {
        return false;
    }
    let Some(tracker) = ctx.deferred_foreign_keys.as_ref() else {
        return false;
    };
    tracker.record(constraint_oid);
    true
}

pub(crate) fn enforce_outbound_foreign_keys(
    relation_name: &str,
    constraints: &[BoundForeignKeyConstraint],
    previous_values: Option<&[Value]>,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for constraint in constraints {
        if !constraint.enforced {
            continue;
        }
        if previous_values.is_some_and(|previous| {
            !key_columns_changed(previous, values, &constraint.column_indexes)
        }) {
            continue;
        }
        let key_values = extract_key_values(values, &constraint.column_indexes);
        if key_values.iter().any(|value| matches!(value, Value::Null)) {
            match constraint.match_type {
                ForeignKeyMatchType::Simple => continue,
                ForeignKeyMatchType::Full => {
                    if key_values.iter().all(|value| matches!(value, Value::Null)) {
                        continue;
                    }
                    return Err(ExecError::ForeignKeyViolation {
                        constraint: constraint.constraint_name.clone(),
                        message: format!(
                            "insert or update on table \"{relation_name}\" violates foreign key constraint \"{}\"",
                            constraint.constraint_name
                        ),
                        detail: Some(
                            "MATCH FULL does not allow mixing of null and nonnull key values."
                                .into(),
                        ),
                    });
                }
                ForeignKeyMatchType::Partial => continue,
            }
        }
        if maybe_defer_constraint(
            ctx,
            constraint.constraint_oid,
            constraint.deferrable,
            constraint.initially_deferred,
        ) {
            continue;
        }
        if referenced_key_exists(constraint, &key_values, ctx)? {
            continue;
        }
        return Err(ExecError::ForeignKeyViolation {
            constraint: constraint.constraint_name.clone(),
            message: format!(
                "insert or update on table \"{relation_name}\" violates foreign key constraint \"{}\"",
                constraint.constraint_name
            ),
            detail: Some(format!(
                "Key ({})=({}) is not present in table \"{}\".",
                constraint.column_names.join(", "),
                render_key_values(&key_values),
                constraint.referenced_relation_name
            )),
        });
    }

    Ok(())
}

pub(crate) fn enforce_inbound_foreign_keys_on_update(
    relation_name: &str,
    constraints: &[BoundReferencedByForeignKey],
    previous_values: &[Value],
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for constraint in constraints {
        if !constraint.enforced {
            continue;
        }
        if !key_columns_changed(
            previous_values,
            values,
            &constraint.referenced_column_indexes,
        ) {
            continue;
        }
        if maybe_defer_constraint(
            ctx,
            constraint.constraint_oid,
            constraint.deferrable,
            constraint.initially_deferred,
        ) {
            continue;
        }
        enforce_inbound_foreign_key(relation_name, constraint, previous_values, ctx)?;
    }
    Ok(())
}

pub(crate) fn enforce_inbound_foreign_keys_on_delete(
    relation_name: &str,
    constraints: &[BoundReferencedByForeignKey],
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for constraint in constraints {
        if !constraint.enforced {
            continue;
        }
        if maybe_defer_constraint(
            ctx,
            constraint.constraint_oid,
            constraint.deferrable,
            constraint.initially_deferred,
        ) {
            continue;
        }
        enforce_inbound_foreign_key(relation_name, constraint, values, ctx)?;
    }
    Ok(())
}

pub(crate) fn enforce_inbound_foreign_key_reference(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if key_values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(());
    }
    if child_row_exists(constraint, key_values, ctx)? {
        return Err(inbound_foreign_key_violation(
            relation_name,
            constraint,
            key_values,
        ));
    }
    Ok(())
}

fn enforce_inbound_foreign_key(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let key_values = extract_key_values(values, &constraint.referenced_column_indexes);
    enforce_inbound_foreign_key_reference(relation_name, constraint, &key_values, ctx)
}

fn inbound_foreign_key_violation(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
) -> ExecError {
    ExecError::ForeignKeyViolation {
        constraint: constraint.constraint_name.clone(),
        message: format!(
            "update or delete on table \"{relation_name}\" violates foreign key constraint \"{}\" on table \"{}\"",
            constraint.constraint_name, constraint.child_relation_name
        ),
        detail: Some(format!(
            "Key ({})=({}) is still referenced from table \"{}\".",
            constraint.referenced_column_names.join(", "),
            render_key_values(key_values),
            constraint.child_relation_name
        )),
    }
}

fn referenced_key_exists(
    constraint: &BoundForeignKeyConstraint,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    index_has_visible_row(
        constraint.referenced_rel,
        &constraint.referenced_index,
        key_values,
        &snapshot,
        ctx,
    )
}

fn child_row_exists(
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    if let Some(index) = &constraint.child_index {
        return index_has_visible_row(constraint.child_rel, index, key_values, &snapshot, ctx);
    }
    heap_has_matching_row(
        constraint.child_rel,
        constraint.child_toast,
        &constraint.child_desc,
        &constraint.child_column_indexes,
        key_values,
        &snapshot,
        ctx,
    )
}

fn index_has_visible_row(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    index: &crate::backend::parser::BoundIndexRelation,
    key_values: &[Value],
    snapshot: &Snapshot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let begin = crate::include::access::amapi::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: snapshot.clone(),
        heap_relation: heap_rel,
        index_relation: index.rel,
        index_desc: index.desc.clone(),
        index_meta: index.index_meta.clone(),
        key_data: build_equality_scan_keys(key_values),
        order_by_data: Vec::new(),
        direction: crate::include::access::relscan::ScanDirection::Forward,
        want_itup: false,
    };
    let mut scan = indexam::index_beginscan(&begin, index.index_meta.am_oid).map_err(|err| {
        ExecError::DetailedError {
            message: "foreign key validation failed".into(),
            detail: Some(format!("index begin scan failed: {err:?}")),
            hint: None,
            sqlstate: "XX000",
        }
    })?;
    let mut seen = HashSet::new();
    let mut found = false;
    loop {
        ctx.check_for_interrupts()?;
        let has_tuple =
            indexam::index_getnext(&mut scan, index.index_meta.am_oid).map_err(|err| {
                ExecError::DetailedError {
                    message: "foreign key validation failed".into(),
                    detail: Some(format!("index scan failed: {err:?}")),
                    hint: None,
                    sqlstate: "XX000",
                }
            })?;
        if !has_tuple {
            break;
        }
        let tid = scan.xs_heaptid.expect("index scan tuple must set heap tid");
        if !seen.insert(tid) {
            continue;
        }
        if heap_fetch_visible_with_txns(
            &ctx.pool,
            ctx.client_id,
            heap_rel,
            tid,
            &ctx.txns,
            snapshot,
        )?
        .is_some()
        {
            found = true;
            break;
        }
    }
    indexam::index_endscan(scan, index.index_meta.am_oid).map_err(|err| {
        ExecError::DetailedError {
            message: "foreign key validation failed".into(),
            detail: Some(format!("index end scan failed: {err:?}")),
            hint: None,
            sqlstate: "XX000",
        }
    })?;
    Ok(found)
}

fn heap_has_matching_row(
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: &crate::backend::executor::RelationDesc,
    key_indexes: &[usize],
    key_values: &[Value],
    snapshot: &Snapshot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, rel, snapshot.clone())?;
    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));
    slot.toast = slot_toast_context(toast, ctx);
    let mut found = false;

    while !found {
        ctx.check_for_interrupts()?;
        let next = heap_scan_prepare_next_page::<ExecError>(
            &*ctx.pool,
            ctx.client_id,
            &ctx.txns,
            &mut scan,
        )?;
        let Some(buffer_id) = next else {
            break;
        };

        let page =
            unsafe { ctx.pool.page_unlocked(buffer_id) }.expect("pinned buffer must be valid");
        let pin = scan
            .pinned_buffer_rc()
            .expect("buffer must be pinned after prepare_next_page");

        while let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, &mut scan) {
            ctx.check_for_interrupts()?;
            slot.kind = SlotKind::BufferHeapTuple {
                desc: Rc::clone(&desc),
                attr_descs: Rc::clone(&attr_descs),
                tid,
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin: Rc::clone(&pin),
            };
            slot.tts_nvalid = 0;
            slot.tts_values.clear();
            slot.decode_offset = 0;
            slot.values()?;
            if row_matches_key(&slot.tts_values, key_indexes, key_values) {
                found = true;
                break;
            }
        }
        drop(pin);
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(found)
}

fn row_matches_key(values: &[Value], key_indexes: &[usize], key_values: &[Value]) -> bool {
    key_indexes.iter().zip(key_values).all(|(index, expected)| {
        values.get(*index).is_some_and(|actual| {
            compare_order_values(actual, expected, None, None, false)
                .expect("foreign-key key comparisons use implicit default collation")
                == Ordering::Equal
        })
    })
}

fn key_columns_changed(previous_values: &[Value], values: &[Value], indexes: &[usize]) -> bool {
    indexes.iter().any(|index| {
        let previous = previous_values.get(*index).unwrap_or(&Value::Null);
        let current = values.get(*index).unwrap_or(&Value::Null);
        compare_order_values(previous, current, None, None, false)
            .expect("foreign-key key comparisons use implicit default collation")
            != Ordering::Equal
    })
}

fn extract_key_values(values: &[Value], indexes: &[usize]) -> Vec<Value> {
    indexes
        .iter()
        .map(|index| {
            values
                .get(*index)
                .cloned()
                .unwrap_or(Value::Null)
                .to_owned_value()
        })
        .collect()
}

fn build_equality_scan_keys(key_values: &[Value]) -> Vec<ScanKeyData> {
    key_values
        .iter()
        .enumerate()
        .map(|(index, value)| ScanKeyData {
            attribute_number: index.saturating_add(1) as i16,
            strategy: 3,
            argument: value.to_owned_value(),
        })
        .collect()
}

fn render_key_values(values: &[Value]) -> String {
    values
        .iter()
        .map(render_key_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_key_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => format!("{v:?}"),
        Value::Bool(v) => v.to_string(),
        Value::InternalChar(v) => v.to_string(),
        Value::TextRef(_, _) | Value::Text(_) | Value::JsonPath(_) => {
            value.as_text().unwrap_or_default().to_string()
        }
        Value::Xml(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Bytea(v) => format!("{v:?}"),
        Value::Date(v) => format!("{v:?}"),
        Value::Time(v) => format!("{v:?}"),
        Value::TimeTz(v) => format!("{v:?}"),
        Value::Timestamp(v) => format!("{v:?}"),
        Value::TimestampTz(v) => format!("{v:?}"),
        Value::Bit(v) => format!("{v:?}"),
        Value::Point(v) => format!("{v:?}"),
        Value::Lseg(v) => format!("{v:?}"),
        Value::Path(v) => format!("{v:?}"),
        Value::Line(v) => format!("{v:?}"),
        Value::Box(v) => format!("{v:?}"),
        Value::Polygon(v) => format!("{v:?}"),
        Value::Circle(v) => format!("{v:?}"),
        Value::Jsonb(v) => format!("{v:?}"),
        Value::TsVector(v) => format!("{v:?}"),
        Value::TsQuery(v) => format!("{v:?}"),
        Value::Array(v) => format!("{v:?}"),
        Value::PgArray(v) => format!("{v:?}"),
        Value::Record(v) => format!("{v:?}"),
        Value::Range(v) => format!("{v:?}"),
        Value::Multirange(v) => format!("{v:?}"),
    }
}

fn slot_toast_context(
    relation: Option<ToastRelationRef>,
    ctx: &ExecutorContext,
) -> Option<crate::include::nodes::execnodes::ToastFetchContext> {
    relation.map(
        |relation| crate::include::nodes::execnodes::ToastFetchContext {
            relation,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        },
    )
}
