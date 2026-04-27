use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;

use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::{CommandId, Snapshot};
use crate::backend::commands::trigger::trigger_is_enabled_for_session;
use crate::backend::executor::exec_tuples::CompiledTupleDecoder;
use crate::backend::parser::{
    BoundForeignKeyConstraint, BoundReferencedByForeignKey, ForeignKeyMatchType,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::catalog::{
    RI_FKEY_CASCADE_DEL_PROC_OID, RI_FKEY_CASCADE_UPD_PROC_OID, RI_FKEY_CHECK_INS_PROC_OID,
    RI_FKEY_CHECK_UPD_PROC_OID, RI_FKEY_NOACTION_DEL_PROC_OID, RI_FKEY_NOACTION_UPD_PROC_OID,
    RI_FKEY_RESTRICT_DEL_PROC_OID, RI_FKEY_RESTRICT_UPD_PROC_OID, RI_FKEY_SETDEFAULT_DEL_PROC_OID,
    RI_FKEY_SETDEFAULT_UPD_PROC_OID, RI_FKEY_SETNULL_DEL_PROC_OID, RI_FKEY_SETNULL_UPD_PROC_OID,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{SlotKind, ToastRelationRef, TupleSlot};

use super::expr_multirange::{
    multirange_contains_multirange, multirange_contains_range, multirange_from_range,
    multirange_overlaps_multirange, multirange_overlaps_range, normalize_multirange,
};
use super::expr_range::{range_contains_range, range_overlap};
use super::permissions::relation_has_table_privilege;
use super::relation_values_visible_for_error_detail;
use super::{ConstraintTiming, ExecError, ExecutorContext, compare_order_values};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertForeignKeyCheckPhase {
    BeforeHeapInsert,
    AfterIndexInsert,
}

fn maybe_defer_constraint(
    ctx: &ExecutorContext,
    constraint_oid: u32,
    deferrable: bool,
    initially_deferred: bool,
) -> bool {
    if ctx.constraint_timing(constraint_oid, deferrable, initially_deferred)
        != ConstraintTiming::Deferred
    {
        return false;
    }
    let Some(tracker) = ctx.deferred_foreign_keys.as_ref() else {
        return false;
    };
    tracker.record(constraint_oid);
    true
}

fn foreign_key_check_trigger_enabled(
    constraint: &BoundForeignKeyConstraint,
    is_update: bool,
    ctx: &ExecutorContext,
) -> bool {
    let proc_oid = if is_update {
        RI_FKEY_CHECK_UPD_PROC_OID
    } else {
        RI_FKEY_CHECK_INS_PROC_OID
    };
    foreign_key_trigger_enabled(constraint.constraint_oid, proc_oid, None, ctx)
}

pub(crate) fn foreign_key_action_trigger_enabled_on_update(
    constraint: &BoundReferencedByForeignKey,
    ctx: &ExecutorContext,
) -> bool {
    foreign_key_trigger_enabled(
        constraint.constraint_oid,
        foreign_key_update_proc_oid(constraint.on_update),
        Some(constraint.child_relation_oid),
        ctx,
    )
}

pub(crate) fn foreign_key_action_trigger_enabled_on_delete(
    constraint: &BoundReferencedByForeignKey,
    ctx: &ExecutorContext,
) -> bool {
    foreign_key_trigger_enabled(
        constraint.constraint_oid,
        foreign_key_delete_proc_oid(constraint.on_delete),
        Some(constraint.child_relation_oid),
        ctx,
    )
}

fn foreign_key_trigger_enabled(
    constraint_oid: u32,
    proc_oid: u32,
    constrrelid: Option<u32>,
    ctx: &ExecutorContext,
) -> bool {
    let Some(catalog) = ctx.catalog.as_deref() else {
        return true;
    };
    let trigger = catalog.class_rows().into_iter().find_map(|relation| {
        catalog
            .trigger_rows_for_relation(relation.oid)
            .into_iter()
            .find(|row| {
                row.tgisinternal
                    && row.tgconstraint == constraint_oid
                    && row.tgfoid == proc_oid
                    && constrrelid.is_none_or(|oid| row.tgconstrrelid == oid)
            })
    });
    trigger
        .as_ref()
        .is_none_or(|row| trigger_is_enabled_for_session(row, ctx.session_replication_role))
}

fn foreign_key_delete_proc_oid(action: crate::include::nodes::parsenodes::ForeignKeyAction) -> u32 {
    match action {
        crate::include::nodes::parsenodes::ForeignKeyAction::Cascade => {
            RI_FKEY_CASCADE_DEL_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::Restrict => {
            RI_FKEY_RESTRICT_DEL_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::SetNull => {
            RI_FKEY_SETNULL_DEL_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::SetDefault => {
            RI_FKEY_SETDEFAULT_DEL_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::NoAction => {
            RI_FKEY_NOACTION_DEL_PROC_OID
        }
    }
}

fn foreign_key_update_proc_oid(action: crate::include::nodes::parsenodes::ForeignKeyAction) -> u32 {
    match action {
        crate::include::nodes::parsenodes::ForeignKeyAction::Cascade => {
            RI_FKEY_CASCADE_UPD_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::Restrict => {
            RI_FKEY_RESTRICT_UPD_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::SetNull => {
            RI_FKEY_SETNULL_UPD_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::SetDefault => {
            RI_FKEY_SETDEFAULT_UPD_PROC_OID
        }
        crate::include::nodes::parsenodes::ForeignKeyAction::NoAction => {
            RI_FKEY_NOACTION_UPD_PROC_OID
        }
    }
}

pub(crate) fn enforce_outbound_foreign_keys_for_insert(
    relation_name: &str,
    relation_rel: crate::backend::storage::smgr::RelFileLocator,
    constraints: &[BoundForeignKeyConstraint],
    values: &[Value],
    phase: InsertForeignKeyCheckPhase,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for constraint in constraints {
        let self_referential = constraint.referenced_rel == relation_rel;
        match phase {
            InsertForeignKeyCheckPhase::BeforeHeapInsert if self_referential => continue,
            InsertForeignKeyCheckPhase::AfterIndexInsert if !self_referential => continue,
            _ => {}
        }
        enforce_outbound_foreign_key(relation_name, constraint, None, values, ctx)?;
    }
    Ok(())
}

pub(crate) fn enforce_outbound_foreign_keys(
    relation_name: &str,
    constraints: &[BoundForeignKeyConstraint],
    previous_values: Option<&[Value]>,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for constraint in constraints {
        enforce_outbound_foreign_key(relation_name, constraint, previous_values, values, ctx)?;
    }

    Ok(())
}

fn enforce_outbound_foreign_key(
    relation_name: &str,
    constraint: &BoundForeignKeyConstraint,
    previous_values: Option<&[Value]>,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if !constraint.enforced {
        return Ok(());
    }
    if !foreign_key_check_trigger_enabled(constraint, previous_values.is_some(), ctx) {
        return Ok(());
    }
    if previous_values
        .is_some_and(|previous| !key_columns_changed(previous, values, &constraint.column_indexes))
    {
        return Ok(());
    }
    let key_values = extract_key_values(values, &constraint.column_indexes);
    if key_values.iter().any(|value| matches!(value, Value::Null)) {
        match constraint.match_type {
            ForeignKeyMatchType::Simple => return Ok(()),
            ForeignKeyMatchType::Full => {
                if key_values.iter().all(|value| matches!(value, Value::Null)) {
                    return Ok(());
                }
                return Err(ExecError::ForeignKeyViolation {
                    constraint: constraint.constraint_name.clone(),
                    message: format!(
                        "insert or update on table \"{relation_name}\" violates foreign key constraint \"{}\"",
                        constraint.constraint_name
                    ),
                    detail: Some(
                        "MATCH FULL does not allow mixing of null and nonnull key values.".into(),
                    ),
                });
            }
            ForeignKeyMatchType::Partial => return Ok(()),
        }
    }
    if maybe_defer_constraint(
        ctx,
        constraint.constraint_oid,
        constraint.deferrable,
        constraint.initially_deferred,
    ) {
        return Ok(());
    }
    check_referenced_table_select_privilege(constraint, ctx)?;
    let exists = if constraint.period_column_index.is_some() {
        temporal_referenced_key_exists(constraint, values, ctx)?
    } else {
        referenced_key_exists(constraint, &key_values, ctx)?
    };
    if exists {
        return Ok(());
    }
    Err(ExecError::ForeignKeyViolation {
        constraint: constraint.constraint_name.clone(),
        message: format!(
            "insert or update on table \"{relation_name}\" violates foreign key constraint \"{}\"",
            constraint.constraint_name
        ),
        detail: Some(format!(
            "Key ({})=({}) is not present in table \"{}\".",
            constraint.column_names.join(", "),
            render_key_values(&key_values, ctx),
            constraint.referenced_relation_name
        )),
    })
}

fn check_referenced_table_select_privilege(
    constraint: &BoundForeignKeyConstraint,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    let Some(catalog) = ctx.catalog.as_ref() else {
        return Ok(());
    };
    let Some(class_row) = catalog.class_row_by_oid(constraint.referenced_relation_oid) else {
        return Ok(());
    };
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if relation_has_table_privilege(
        &class_row,
        &authid_rows,
        &auth_members_rows,
        ctx.current_user_oid,
        'r',
    ) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for table {}", class_row.relname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
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
        if !foreign_key_action_trigger_enabled_on_update(constraint, ctx) {
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
        if constraint.referenced_period_column_index.is_some() {
            let key_values =
                extract_key_values(previous_values, &constraint.referenced_column_indexes);
            if temporal_child_reference_would_be_invalid(
                constraint,
                previous_values,
                Some(values),
                ctx,
            )? {
                return Err(inbound_foreign_key_violation(
                    relation_name,
                    constraint,
                    &key_values,
                    ctx,
                ));
            }
        } else {
            enforce_inbound_foreign_key(relation_name, constraint, previous_values, ctx)?;
        }
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
        if !foreign_key_action_trigger_enabled_on_delete(constraint, ctx) {
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
        if constraint.referenced_period_column_index.is_some() {
            let key_values = extract_key_values(values, &constraint.referenced_column_indexes);
            if temporal_child_reference_would_be_invalid(constraint, values, None, ctx)? {
                return Err(inbound_foreign_key_violation(
                    relation_name,
                    constraint,
                    &key_values,
                    ctx,
                ));
            }
        } else {
            enforce_inbound_foreign_key(relation_name, constraint, values, ctx)?;
        }
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
            ctx,
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
    ctx: &ExecutorContext,
) -> ExecError {
    let detail =
        if relation_values_visible_for_error_detail(constraint.referenced_relation_oid, ctx) {
            format!(
                "Key ({})=({}) is still referenced from table \"{}\".",
                constraint.referenced_column_names.join(", "),
                render_key_values(key_values, ctx),
                constraint.child_relation_name
            )
        } else {
            format!(
                "Key is still referenced from table \"{}\".",
                constraint.child_relation_name
            )
        };
    ExecError::ForeignKeyViolation {
        constraint: constraint.constraint_name.clone(),
        message: format!(
            "update or delete on table \"{relation_name}\" violates foreign key constraint \"{}\" on table \"{}\"",
            constraint.constraint_name, constraint.child_relation_name
        ),
        detail: Some(detail),
    }
}

fn referenced_key_exists(
    constraint: &BoundForeignKeyConstraint,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    let key_values = referenced_key_values_in_index_order(constraint, key_values)?;
    index_has_visible_row(
        constraint.referenced_rel,
        &constraint.referenced_index,
        &key_values,
        &snapshot,
        ctx,
    )
}

fn referenced_key_values_in_index_order(
    constraint: &BoundForeignKeyConstraint,
    key_values: &[Value],
) -> Result<Vec<Value>, ExecError> {
    let key_count = usize::try_from(constraint.referenced_index.index_meta.indnkeyatts.max(0))
        .map_err(|_| foreign_key_internal_error("invalid referenced index key count"))?;
    if key_count > constraint.referenced_index.index_meta.indkey.len() {
        return Err(foreign_key_internal_error(
            "referenced index key count exceeds index columns",
        ));
    }
    if key_count != constraint.referenced_column_indexes.len() || key_count != key_values.len() {
        return Err(foreign_key_internal_error(
            "referenced index key count does not match foreign key columns",
        ));
    }

    let mut reordered = Vec::with_capacity(key_count);
    for index_attnum in constraint
        .referenced_index
        .index_meta
        .indkey
        .iter()
        .take(key_count)
    {
        if *index_attnum <= 0 {
            return Err(foreign_key_internal_error(
                "referenced foreign key index uses an expression key",
            ));
        }
        let Some(position) = constraint
            .referenced_column_indexes
            .iter()
            .position(|column_index| (*column_index as i16) + 1 == *index_attnum)
        else {
            return Err(foreign_key_internal_error(
                "referenced index columns do not match foreign key columns",
            ));
        };
        reordered.push(
            key_values
                .get(position)
                .cloned()
                .ok_or_else(|| foreign_key_internal_error("missing foreign key value"))?,
        );
    }
    Ok(reordered)
}

fn foreign_key_internal_error(detail: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: "foreign key validation failed".into(),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "XX000",
    }
}

fn temporal_referenced_key_exists(
    constraint: &BoundForeignKeyConstraint,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(period_index) = constraint.period_column_index else {
        return Ok(false);
    };
    let Some(referenced_period_index) = constraint.referenced_period_column_index else {
        return Ok(false);
    };
    let Some(child_period) = values.get(period_index) else {
        return Ok(false);
    };
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    let parent_periods = collect_temporal_parent_periods(
        constraint.referenced_rel,
        constraint.referenced_toast,
        &constraint.referenced_desc,
        &constraint.column_indexes,
        Some(period_index),
        values,
        &constraint.referenced_column_indexes,
        Some(referenced_period_index),
        None,
        None,
        &snapshot,
        ctx,
    )?;
    temporal_periods_cover(&parent_periods, child_period)
}

fn temporal_child_reference_would_be_invalid(
    constraint: &BoundReferencedByForeignKey,
    old_parent_values: &[Value],
    replacement_parent_values: Option<&[Value]>,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(child_period_index) = constraint.child_period_column_index else {
        return Ok(false);
    };
    let Some(referenced_period_index) = constraint.referenced_period_column_index else {
        return Ok(false);
    };
    let Some(old_parent_period) = old_parent_values.get(referenced_period_index) else {
        return Ok(false);
    };
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        constraint.child_rel,
        snapshot.clone(),
    )?;
    let desc = Rc::new(constraint.child_desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));
    slot.toast = slot_toast_context(constraint.child_toast, ctx);
    let mut invalid = false;

    while !invalid {
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
        let mut pending_child_values = Vec::new();

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
            if !values_match_cross_indexes(
                &slot.tts_values,
                &constraint.child_column_indexes,
                Some(child_period_index),
                old_parent_values,
                &constraint.referenced_column_indexes,
                Some(referenced_period_index),
            ) {
                continue;
            }
            let Some(child_period) = slot.tts_values.get(child_period_index) else {
                continue;
            };
            if !periods_overlap(child_period, old_parent_period)? {
                continue;
            }
            pending_child_values.push(
                slot.tts_values
                    .iter()
                    .map(Value::to_owned_value)
                    .collect::<Vec<_>>(),
            );
        }
        drop(pin);

        for child_values in pending_child_values {
            if !temporal_child_period_still_covered(
                constraint,
                &child_values,
                old_parent_values,
                replacement_parent_values,
                &snapshot,
                ctx,
            )? {
                invalid = true;
                break;
            }
        }
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(invalid)
}

fn temporal_child_period_still_covered(
    constraint: &BoundReferencedByForeignKey,
    child_values: &[Value],
    excluded_parent_values: &[Value],
    replacement_parent_values: Option<&[Value]>,
    snapshot: &Snapshot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let Some(child_period_index) = constraint.child_period_column_index else {
        return Ok(false);
    };
    let Some(referenced_period_index) = constraint.referenced_period_column_index else {
        return Ok(false);
    };
    let Some(child_period) = child_values.get(child_period_index) else {
        return Ok(false);
    };
    let mut parent_periods = collect_temporal_parent_periods(
        constraint.referenced_rel,
        constraint.referenced_toast,
        &constraint.referenced_desc,
        &constraint.child_column_indexes,
        Some(child_period_index),
        child_values,
        &constraint.referenced_column_indexes,
        Some(referenced_period_index),
        Some(excluded_parent_values),
        replacement_parent_values,
        snapshot,
        ctx,
    )?;
    if let Some(replacement) = replacement_parent_values
        && values_match_cross_indexes(
            child_values,
            &constraint.child_column_indexes,
            Some(child_period_index),
            replacement,
            &constraint.referenced_column_indexes,
            Some(referenced_period_index),
        )
        && let Some(period) = replacement.get(referenced_period_index)
    {
        parent_periods.push(period.to_owned_value());
    }
    temporal_periods_cover(&parent_periods, child_period)
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

#[allow(clippy::too_many_arguments)]
fn collect_temporal_parent_periods(
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: &crate::backend::executor::RelationDesc,
    child_key_indexes: &[usize],
    child_period_index: Option<usize>,
    child_values: &[Value],
    parent_key_indexes: &[usize],
    parent_period_index: Option<usize>,
    excluded_parent_values: Option<&[Value]>,
    replacement_parent_values: Option<&[Value]>,
    snapshot: &Snapshot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, rel, snapshot.clone())?;
    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));
    slot.toast = slot_toast_context(toast, ctx);
    let mut periods = Vec::new();

    loop {
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
            let _ = replacement_parent_values;
            if excluded_parent_values.is_some_and(|excluded| {
                row_matches_key(
                    &slot.tts_values,
                    parent_key_indexes,
                    &extract_key_values(excluded, parent_key_indexes),
                )
            }) {
                continue;
            }
            if !values_match_cross_indexes(
                child_values,
                child_key_indexes,
                child_period_index,
                &slot.tts_values,
                parent_key_indexes,
                parent_period_index,
            ) {
                continue;
            }
            if let Some(period_index) = parent_period_index
                && let Some(period) = slot.tts_values.get(period_index)
            {
                periods.push(period.to_owned_value());
            }
        }
        drop(pin);
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(periods)
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

fn values_match_cross_indexes(
    left_values: &[Value],
    left_indexes: &[usize],
    left_period_index: Option<usize>,
    right_values: &[Value],
    right_indexes: &[usize],
    right_period_index: Option<usize>,
) -> bool {
    left_indexes
        .iter()
        .zip(right_indexes)
        .filter(|(left, right)| {
            Some(**left) != left_period_index && Some(**right) != right_period_index
        })
        .all(|(left, right)| {
            left_values
                .get(*left)
                .zip(right_values.get(*right))
                .is_some_and(|(left, right)| {
                    compare_order_values(left, right, None, None, false)
                        .expect("foreign-key key comparisons use implicit default collation")
                        == Ordering::Equal
                })
        })
}

fn periods_overlap(left: &Value, right: &Value) -> Result<bool, ExecError> {
    match (left, right) {
        (Value::Range(left), Value::Range(right)) => Ok(range_overlap(left, right)),
        (Value::Multirange(left), Value::Range(right)) => {
            Ok(multirange_overlaps_range(left, right))
        }
        (Value::Range(left), Value::Multirange(right)) => {
            Ok(multirange_overlaps_range(right, left))
        }
        (Value::Multirange(left), Value::Multirange(right)) => {
            Ok(multirange_overlaps_multirange(left, right))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Err(ExecError::TypeMismatch {
            op: "PERIOD foreign key",
            left: left.to_owned_value(),
            right: right.to_owned_value(),
        }),
    }
}

fn temporal_periods_cover(
    parent_periods: &[Value],
    child_period: &Value,
) -> Result<bool, ExecError> {
    match child_period {
        Value::Range(child) => {
            let mut ranges = Vec::new();
            for period in parent_periods {
                match period {
                    Value::Range(range) => ranges.push(range.clone()),
                    Value::Multirange(multirange) => ranges.extend(multirange.ranges.clone()),
                    Value::Null => {}
                    other => {
                        return Err(ExecError::TypeMismatch {
                            op: "PERIOD foreign key",
                            left: other.to_owned_value(),
                            right: child_period.to_owned_value(),
                        });
                    }
                }
            }
            if ranges.is_empty() {
                return Ok(false);
            }
            match multirange_from_range(child) {
                Ok(multirange) => {
                    let parent = normalize_multirange(multirange.multirange_type, ranges)?;
                    Ok(multirange_contains_range(&parent, child))
                }
                Err(_) => Ok(ranges
                    .iter()
                    .any(|parent| range_contains_range(parent, child))),
            }
        }
        Value::Multirange(child) => {
            let mut ranges = Vec::new();
            for period in parent_periods {
                match period {
                    Value::Range(range) => ranges.push(range.clone()),
                    Value::Multirange(multirange) => ranges.extend(multirange.ranges.clone()),
                    Value::Null => {}
                    other => {
                        return Err(ExecError::TypeMismatch {
                            op: "PERIOD foreign key",
                            left: other.to_owned_value(),
                            right: child_period.to_owned_value(),
                        });
                    }
                }
            }
            if ranges.is_empty() {
                return Ok(false);
            }
            let parent = normalize_multirange(child.multirange_type, ranges)?;
            Ok(multirange_contains_multirange(&parent, child))
        }
        Value::Null => Ok(true),
        other => Err(ExecError::TypeMismatch {
            op: "PERIOD foreign key",
            left: other.to_owned_value(),
            right: Value::Null,
        }),
    }
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

fn render_key_values(values: &[Value], ctx: &ExecutorContext) -> String {
    values
        .iter()
        .map(|value| render_key_value(value, ctx))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_key_value(value: &Value, ctx: &ExecutorContext) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => crate::backend::executor::render_pg_lsn_text(*v),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => format!("{v:?}"),
        Value::Interval(v) => format!("{v:?}"),
        Value::Uuid(v) => crate::backend::executor::value_io::render_uuid_text(v),
        Value::Bool(v) => v.to_string(),
        Value::InternalChar(v) => v.to_string(),
        Value::EnumOid(v) => ctx
            .catalog
            .as_deref()
            .and_then(|catalog| catalog.enum_label_by_oid(*v))
            .unwrap_or_else(|| v.to_string()),
        Value::TextRef(_, _) | Value::Text(_) | Value::JsonPath(_) => {
            value.as_text().unwrap_or_default().to_string()
        }
        Value::Xml(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Bytea(v) => format!("{v:?}"),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => crate::backend::executor::render_macaddr_text(v),
        Value::MacAddr8(v) => crate::backend::executor::render_macaddr8_text(v),
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
