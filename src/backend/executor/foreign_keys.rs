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
    BoundForeignKeyConstraint, BoundIndexRelation, BoundReferencedByForeignKey, BoundRelation,
    CatalogLookup, ForeignKeyAction, ForeignKeyMatchType,
};
use crate::include::catalog::{RI_FKEY_CHECK_INS_PROC_OID, RI_FKEY_CHECK_UPD_PROC_OID};
use crate::include::nodes::datum::{IndirectVarlenaValue, Value};
use crate::include::nodes::execnodes::{SlotKind, ToastRelationRef, TupleSlot};

use super::permissions::relation_has_table_privilege;
use super::relation_values_visible_for_error_detail;
use super::{ConstraintTiming, ExecError, ExecutorContext};

use pgrust_executor::{
    ForeignKeyHelperError, InboundForeignKeyViolationInfo, InsertForeignKeyCheckPhase,
    build_equality_scan_keys, extract_key_values, foreign_key_delete_proc_oid,
    foreign_key_update_proc_oid, inbound_foreign_key_violation_message,
    inbound_restrict_foreign_key_violation_message, key_columns_changed,
    map_column_indexes_by_name, periods_overlap, row_matches_key, temporal_periods_cover,
    values_match_cross_indexes,
};

struct RootForeignKeyValueRenderContext<'a>(&'a ExecutorContext);

impl pgrust_executor::ForeignKeyValueRenderContext for RootForeignKeyValueRenderContext<'_> {
    fn enum_label_by_oid(&self, oid: u32) -> Option<String> {
        self.0
            .catalog
            .as_deref()
            .and_then(|catalog| catalog.enum_label_by_oid(oid))
    }

    fn decode_indirect_varlena(&self, indirect: &IndirectVarlenaValue) -> Option<Value> {
        crate::backend::executor::value_io::indirect_varlena_to_value(indirect).ok()
    }

    fn datetime_config(&self) -> &pgrust_expr::DateTimeConfig {
        &self.0.datetime_config
    }
}

impl From<ForeignKeyHelperError> for ExecError {
    fn from(err: ForeignKeyHelperError) -> Self {
        match err {
            ForeignKeyHelperError::Internal(message) => foreign_key_internal_error(message),
            ForeignKeyHelperError::Expr(err) => err.into(),
            ForeignKeyHelperError::TypeMismatch { op, left, right } => {
                ExecError::TypeMismatch { op, left, right }
            }
        }
    }
}

fn maybe_defer_constraint(
    ctx: &ExecutorContext,
    relation_name: &str,
    constraint: &BoundForeignKeyConstraint,
    previous_values: Option<&[Value]>,
    values: &[Value],
) -> bool {
    if ctx.constraint_timing(
        constraint.constraint_oid,
        constraint.deferrable,
        constraint.initially_deferred,
    ) != ConstraintTiming::Deferred
    {
        return false;
    }
    let Some(tracker) = ctx.deferred_foreign_keys.as_ref() else {
        return false;
    };
    if let Some(previous_values) = previous_values {
        tracker.cancel_foreign_key_check(constraint.constraint_oid, previous_values);
    }
    tracker.record_foreign_key_check(
        constraint.constraint_oid,
        relation_name.to_string(),
        values.iter().map(Value::to_owned_value).collect::<Vec<_>>(),
    );
    true
}

fn maybe_defer_parent_constraint(
    ctx: &ExecutorContext,
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    old_parent_values: &[Value],
    replacement_parent_values: Option<&[Value]>,
) -> bool {
    if ctx.constraint_timing(
        constraint.constraint_oid,
        constraint.deferrable,
        constraint.initially_deferred,
    ) != ConstraintTiming::Deferred
    {
        return false;
    }
    let Some(tracker) = ctx.deferred_foreign_keys.as_ref() else {
        return false;
    };
    tracker.record_parent_foreign_key_check(
        constraint.constraint_oid,
        relation_name.to_string(),
        old_parent_values
            .iter()
            .map(Value::to_owned_value)
            .collect::<Vec<_>>(),
        replacement_parent_values
            .map(|values| values.iter().map(Value::to_owned_value).collect::<Vec<_>>()),
    );
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

pub(crate) fn validate_outbound_foreign_key_for_ddl(
    relation_name: &str,
    constraint: &BoundForeignKeyConstraint,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    enforce_outbound_foreign_key_impl(relation_name, constraint, None, values, ctx, false, false)
}

fn enforce_outbound_foreign_key(
    relation_name: &str,
    constraint: &BoundForeignKeyConstraint,
    previous_values: Option<&[Value]>,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    enforce_outbound_foreign_key_impl(
        relation_name,
        constraint,
        previous_values,
        values,
        ctx,
        true,
        true,
    )
}

fn enforce_outbound_foreign_key_impl(
    relation_name: &str,
    constraint: &BoundForeignKeyConstraint,
    previous_values: Option<&[Value]>,
    values: &[Value],
    ctx: &mut ExecutorContext,
    check_triggers: bool,
    check_select_privilege: bool,
) -> Result<(), ExecError> {
    if !constraint.enforced {
        return Ok(());
    }
    if check_triggers
        && !foreign_key_check_trigger_enabled(constraint, previous_values.is_some(), ctx)
    {
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
    if maybe_defer_constraint(ctx, relation_name, constraint, previous_values, values) {
        return Ok(());
    }
    if check_select_privilege {
        check_referenced_table_select_privilege(constraint, ctx)?;
    }
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
        if maybe_defer_parent_constraint(
            ctx,
            relation_name,
            constraint,
            previous_values,
            Some(values),
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
        } else if constraint.on_update == ForeignKeyAction::Restrict {
            enforce_inbound_foreign_key_restrict(relation_name, constraint, previous_values, ctx)?;
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
        if maybe_defer_parent_constraint(ctx, relation_name, constraint, values, None) {
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
        } else if constraint.on_delete == ForeignKeyAction::Restrict {
            enforce_inbound_foreign_key_restrict(relation_name, constraint, values, ctx)?;
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

pub(crate) fn enforce_deferred_inbound_foreign_key_check(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    old_parent_values: &[Value],
    replacement_parent_values: Option<&[Value]>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let key_values = extract_key_values(old_parent_values, &constraint.referenced_column_indexes);
    if key_values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(());
    }
    if constraint.referenced_period_column_index.is_some() {
        if temporal_child_reference_would_be_invalid(
            constraint,
            old_parent_values,
            replacement_parent_values,
            ctx,
        )? {
            return Err(inbound_foreign_key_violation(
                relation_name,
                constraint,
                &key_values,
                ctx,
            ));
        }
        return Ok(());
    }
    if replacement_parent_values.is_some_and(|replacement| {
        row_matches_key(
            replacement,
            &constraint.referenced_column_indexes,
            &key_values,
        )
    }) {
        return Ok(());
    }
    if referenced_parent_key_exists_for_no_action(constraint, &key_values, ctx)? {
        return Ok(());
    }
    enforce_inbound_foreign_key_reference(relation_name, constraint, &key_values, ctx)
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

fn enforce_inbound_foreign_key_restrict(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let key_values = extract_key_values(values, &constraint.referenced_column_indexes);
    if key_values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(());
    }
    if child_row_exists(constraint, &key_values, ctx)? {
        return Err(inbound_restrict_foreign_key_violation(
            relation_name,
            constraint,
            &key_values,
            ctx,
        ));
    }
    Ok(())
}

fn referenced_parent_key_exists_for_no_action(
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    snapshot.heap_current_cid = None;
    let partitioned_catalog = ctx.catalog.as_ref().and_then(|catalog| {
        catalog
            .relation_by_oid(constraint.referenced_relation_oid)
            .is_some_and(|relation| relation.relkind == 'p')
            .then(|| catalog.clone())
    });
    if let Some(catalog) = partitioned_catalog {
        for leaf in partition_leaf_relations(catalog.as_ref(), constraint.referenced_relation_oid)?
        {
            let leaf_key_indexes = map_column_indexes_by_name(
                &constraint.referenced_desc,
                &leaf.desc,
                &constraint.referenced_column_indexes,
            )?;
            if heap_has_matching_row(
                leaf.rel,
                leaf.toast,
                &leaf.desc,
                &leaf_key_indexes,
                key_values,
                &snapshot,
                ctx,
            )? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    heap_has_matching_row(
        constraint.referenced_rel,
        constraint.referenced_toast,
        &constraint.referenced_desc,
        &constraint.referenced_column_indexes,
        key_values,
        &snapshot,
        ctx,
    )
}

fn inbound_foreign_key_violation(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &ExecutorContext,
) -> ExecError {
    let rendered_key_values =
        relation_values_visible_for_error_detail(constraint.referenced_relation_oid, ctx)
            .then(|| render_key_values(key_values, ctx));
    let violation = inbound_foreign_key_violation_message(
        inbound_foreign_key_violation_info(relation_name, constraint),
        rendered_key_values.as_deref(),
    );
    ExecError::ForeignKeyViolation {
        constraint: violation.constraint,
        message: violation.message,
        detail: violation.detail,
    }
}

fn inbound_restrict_foreign_key_violation(
    relation_name: &str,
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &ExecutorContext,
) -> ExecError {
    let rendered_key_values =
        relation_values_visible_for_error_detail(constraint.referenced_relation_oid, ctx)
            .then(|| render_key_values(key_values, ctx));
    let violation = inbound_restrict_foreign_key_violation_message(
        inbound_foreign_key_violation_info(relation_name, constraint),
        rendered_key_values.as_deref(),
    );
    ExecError::ForeignKeyViolation {
        constraint: violation.constraint,
        message: violation.message,
        detail: violation.detail,
    }
}

fn inbound_foreign_key_violation_info<'a>(
    relation_name: &'a str,
    constraint: &'a BoundReferencedByForeignKey,
) -> InboundForeignKeyViolationInfo<'a> {
    InboundForeignKeyViolationInfo {
        relation_name,
        constraint_name: &constraint.display_constraint_name,
        child_relation_name: &constraint.display_child_relation_name,
        referenced_column_names: &constraint.referenced_column_names,
    }
}

fn referenced_key_exists(
    constraint: &BoundForeignKeyConstraint,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    snapshot.heap_current_cid = None;
    let partitioned_catalog = ctx.catalog.as_ref().and_then(|catalog| {
        catalog
            .relation_by_oid(constraint.referenced_relation_oid)
            .is_some_and(|relation| relation.relkind == 'p')
            .then(|| catalog.clone())
    });
    if let Some(catalog) = partitioned_catalog {
        return partitioned_referenced_key_exists(
            constraint,
            key_values,
            &snapshot,
            catalog.as_ref(),
            ctx,
        );
    }
    let key_values = referenced_key_values_in_index_order(constraint, key_values)?;
    index_has_visible_row(
        constraint.referenced_rel,
        &constraint.referenced_index,
        &key_values,
        &snapshot,
        ctx,
    )
}

fn partitioned_referenced_key_exists(
    constraint: &BoundForeignKeyConstraint,
    key_values: &[Value],
    snapshot: &Snapshot,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let leaves = partition_leaf_relations(catalog, constraint.referenced_relation_oid)?;
    for leaf in leaves {
        let leaf_key_indexes = map_column_indexes_by_name(
            &constraint.referenced_desc,
            &leaf.desc,
            &constraint.referenced_column_indexes,
        )?;
        let mut checked_index = false;
        for index in catalog.index_relations_for_heap(leaf.relation_oid) {
            let Some(leaf_key_values) = key_values_in_index_order_for_column_indexes(
                &index,
                &leaf_key_indexes,
                key_values,
            )?
            else {
                continue;
            };
            checked_index = true;
            if index_has_visible_row(leaf.rel, &index, &leaf_key_values, snapshot, ctx)? {
                return Ok(true);
            }
            break;
        }
        if checked_index {
            continue;
        }
        if heap_has_matching_row(
            leaf.rel,
            leaf.toast,
            &leaf.desc,
            &leaf_key_indexes,
            key_values,
            snapshot,
            ctx,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn key_values_in_index_order_for_column_indexes(
    index: &BoundIndexRelation,
    column_indexes: &[usize],
    key_values: &[Value],
) -> Result<Option<Vec<Value>>, ExecError> {
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0))
        .map_err(|_| foreign_key_internal_error("invalid referenced index key count"))?;
    if key_count != column_indexes.len()
        || key_count != key_values.len()
        || key_count > index.index_meta.indkey.len()
    {
        return Ok(None);
    }

    let mut reordered = Vec::with_capacity(key_count);
    for index_attnum in index.index_meta.indkey.iter().take(key_count) {
        if *index_attnum <= 0 {
            return Ok(None);
        }
        let Some(position) = column_indexes
            .iter()
            .position(|column_index| (*column_index as i16) + 1 == *index_attnum)
        else {
            return Ok(None);
        };
        reordered.push(
            key_values
                .get(position)
                .cloned()
                .ok_or_else(|| foreign_key_internal_error("missing foreign key value"))?,
        );
    }
    Ok(Some(reordered))
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
    snapshot.heap_current_cid = None;
    if let Some(catalog) = ctx.catalog.clone()
        && catalog
            .relation_by_oid(constraint.referenced_relation_oid)
            .is_some_and(|relation| relation.relkind == 'p')
    {
        let mut parent_periods = Vec::new();
        for leaf in partition_leaf_relations(catalog.as_ref(), constraint.referenced_relation_oid)?
        {
            let leaf_key_indexes = map_column_indexes_by_name(
                &constraint.referenced_desc,
                &leaf.desc,
                &constraint.referenced_column_indexes,
            )?;
            let leaf_period_index = map_column_indexes_by_name(
                &constraint.referenced_desc,
                &leaf.desc,
                &[referenced_period_index],
            )?
            .into_iter()
            .next();
            parent_periods.extend(collect_temporal_parent_periods(
                leaf.rel,
                leaf.toast,
                &leaf.desc,
                &constraint.column_indexes,
                Some(period_index),
                values,
                &leaf_key_indexes,
                leaf_period_index,
                None,
                None,
                &snapshot,
                ctx,
            )?);
        }
        return Ok(temporal_periods_cover(&parent_periods, child_period)?);
    }
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
    Ok(temporal_periods_cover(&parent_periods, child_period)?)
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
    snapshot.heap_current_cid = None;
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
    Ok(temporal_periods_cover(&parent_periods, child_period)?)
}

fn child_row_exists(
    constraint: &BoundReferencedByForeignKey,
    key_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let mut snapshot = ctx.snapshot.clone();
    snapshot.current_cid = CommandId::MAX;
    snapshot.heap_current_cid = None;
    let partitioned_catalog = ctx.catalog.as_ref().and_then(|catalog| {
        catalog
            .relation_by_oid(constraint.child_relation_oid)
            .is_some_and(|relation| relation.relkind == 'p')
            .then(|| catalog.clone())
    });
    if let Some(catalog) = partitioned_catalog {
        let leaves = partition_leaf_relations(catalog.as_ref(), constraint.child_relation_oid)?;
        for leaf in leaves {
            let leaf_key_indexes = map_column_indexes_by_name(
                &constraint.child_desc,
                &leaf.desc,
                &constraint.child_column_indexes,
            )?;
            if heap_has_matching_row(
                leaf.rel,
                leaf.toast,
                &leaf.desc,
                &leaf_key_indexes,
                key_values,
                &snapshot,
                ctx,
            )? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
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

fn partition_leaf_relations(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<BoundRelation>, ExecError> {
    let mut children = catalog.inheritance_children(relation_oid);
    children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    let mut leaves = Vec::new();
    for child in children.into_iter().filter(|row| !row.inhdetachpending) {
        let relation = catalog
            .relation_by_oid(child.inhrelid)
            .ok_or_else(|| foreign_key_internal_error("missing partition relation"))?;
        if relation.relkind == 'p' {
            leaves.extend(partition_leaf_relations(catalog, relation.relation_oid)?);
        } else {
            leaves.push(relation);
        }
    }
    Ok(leaves)
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

fn render_key_values(values: &[Value], ctx: &ExecutorContext) -> String {
    pgrust_executor::render_key_values(values, &RootForeignKeyValueRenderContext(ctx))
}

fn render_key_value(value: &Value, ctx: &ExecutorContext) -> String {
    pgrust_executor::render_key_value(value, &RootForeignKeyValueRenderContext(ctx))
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
