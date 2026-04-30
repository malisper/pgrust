use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::value_io::format_failing_row_detail;
use crate::backend::executor::{
    ExecError, ExecutorContext, RelationDesc, TupleSlot, compare_order_values, eval_expr,
    execute_scalar_function_value_call, render_datetime_value_text_with_config,
};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec,
    PartitionRangeDatumValue, PartitionStrategy, SerializedPartitionValue, SqlType,
    deserialize_partition_bound, partition_value_to_value, relation_partition_spec,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::catalog::{
    ANYARRAYOID, ANYOID, BOOTSTRAP_SUPERUSER_OID, CONSTRAINT_CHECK, CONSTRAINT_NOTNULL,
    PgConstraintRow,
};
use crate::include::nodes::datum::Value;

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn direct_partition_children(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<BoundRelation>, ExecError> {
    let mut inherits = catalog.inheritance_children(relation_oid);
    inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    inherits
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .map(|row| {
            catalog
                .relation_by_oid(row.inhrelid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("missing partition relation {}", row.inhrelid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

const PARTITION_CACHED_FIND_THRESHOLD: usize = 16;

#[derive(Default)]
pub(crate) struct PartitionTupleRouting {
    partition_dispatch_info: HashMap<u32, PartitionDispatch>,
}

impl PartitionTupleRouting {
    fn dispatch_info_for_relation(
        &mut self,
        catalog: &dyn CatalogLookup,
        relation: &BoundRelation,
    ) -> Result<&mut PartitionDispatch, ExecError> {
        if !self
            .partition_dispatch_info
            .contains_key(&relation.relation_oid)
        {
            let dispatch = exec_init_partition_dispatch_info(catalog, relation)?;
            self.partition_dispatch_info
                .insert(relation.relation_oid, dispatch);
        }
        Ok(self
            .partition_dispatch_info
            .get_mut(&relation.relation_oid)
            .expect("partition dispatch was just cached"))
    }
}

#[derive(Debug, Clone)]
struct PartitionDispatch {
    reldesc: BoundRelation,
    key: LoweredPartitionSpec,
    partdesc: PartitionDesc,
}

#[derive(Debug, Clone)]
struct PartitionDesc {
    children: Vec<PartitionDescEntry>,
    boundinfo: PartitionBoundInfo,
    last_found_datum_index: Option<usize>,
    last_found_part_index: Option<usize>,
    last_found_count: usize,
}

impl PartitionDesc {
    fn cached_partition_index(&self) -> Option<usize> {
        if self.last_found_count >= PARTITION_CACHED_FIND_THRESHOLD {
            self.last_found_part_index
        } else {
            None
        }
    }

    fn record_partition_match(&mut self, part_index: usize) {
        if self.last_found_datum_index == Some(part_index) {
            self.last_found_count += 1;
        } else {
            self.last_found_datum_index = Some(part_index);
            self.last_found_part_index = Some(part_index);
            self.last_found_count = 1;
        }
    }
}

#[derive(Debug, Clone)]
struct PartitionDescEntry {
    reldesc: BoundRelation,
    bound: PartitionBoundSpec,
    is_leaf: bool,
}

#[derive(Debug, Clone, Default)]
struct PartitionBoundInfo {
    indexes: Vec<usize>,
    default_index: Option<usize>,
}

pub(crate) fn exec_setup_partition_tuple_routing(
    catalog: &dyn CatalogLookup,
    root: &BoundRelation,
) -> Result<PartitionTupleRouting, ExecError> {
    let mut proute = PartitionTupleRouting::default();
    if root.relkind == 'p' {
        proute.dispatch_info_for_relation(catalog, root)?;
    }
    Ok(proute)
}

fn exec_init_partition_dispatch_info(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> Result<PartitionDispatch, ExecError> {
    let key = relation_partition_spec(relation).map_err(ExecError::Parse)?;
    let mut children = Vec::new();
    let mut boundinfo = PartitionBoundInfo::default();
    for child in direct_partition_children(catalog, relation.relation_oid)? {
        let bound = child_partition_bound(&child)?;
        let index = children.len();
        if bound.is_default() {
            if boundinfo.default_index.is_none() {
                boundinfo.default_index = Some(index);
            }
        } else {
            boundinfo.indexes.push(index);
        }
        children.push(PartitionDescEntry {
            is_leaf: child.partitioned_table.is_none(),
            reldesc: child,
            bound,
        });
    }
    Ok(PartitionDispatch {
        reldesc: relation.clone(),
        key,
        partdesc: PartitionDesc {
            children,
            boundinfo,
            last_found_datum_index: None,
            last_found_part_index: None,
            last_found_count: 0,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionTreeEntry {
    pub relid: u32,
    pub parentrelid: Option<u32>,
    pub isleaf: bool,
    pub level: i32,
}

fn relkind_has_partitions(relkind: char) -> bool {
    matches!(relkind, 'p' | 'I')
}

fn relation_can_participate_in_partition_tree(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> bool {
    catalog
        .relation_by_oid(relation_oid)
        .is_some_and(|relation| relation.relispartition || relkind_has_partitions(relation.relkind))
}

fn declarative_parent(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> Result<Option<BoundRelation>, ExecError> {
    let parent_oid = catalog
        .inheritance_parents(relation.relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .find_map(|row| {
            catalog
                .relation_by_oid(row.inhparent)
                .filter(|parent| relkind_has_partitions(parent.relkind))
                .map(|parent| parent.relation_oid)
        });
    parent_oid
        .map(|oid| {
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("missing partitioned parent {}", oid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .transpose()
}

pub(crate) fn partition_parent_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, ExecError> {
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(None);
    };
    declarative_parent(catalog, &relation).map(|parent| parent.map(|parent| parent.relation_oid))
}

pub(crate) fn partition_ancestor_oids(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<u32>, ExecError> {
    if !relation_can_participate_in_partition_tree(catalog, relation_oid) {
        return Ok(Vec::new());
    }

    let mut ancestors = Vec::new();
    let mut current = Some(relation_oid);
    while let Some(relid) = current {
        ancestors.push(relid);
        current = partition_parent_oid(catalog, relid)?;
    }
    Ok(ancestors)
}

pub(crate) fn partition_root_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, ExecError> {
    Ok(partition_ancestor_oids(catalog, relation_oid)?
        .into_iter()
        .last())
}

pub(crate) fn partition_tree_entries(
    catalog: &dyn CatalogLookup,
    root_oid: u32,
) -> Result<Vec<PartitionTreeEntry>, ExecError> {
    if !relation_can_participate_in_partition_tree(catalog, root_oid) {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut queue = VecDeque::from([(root_oid, 0_i32)]);
    while let Some((relation_oid, level)) = queue.pop_front() {
        let Some(relation) = catalog.relation_by_oid(relation_oid) else {
            continue;
        };
        entries.push(PartitionTreeEntry {
            relid: relation_oid,
            parentrelid: partition_parent_oid(catalog, relation_oid)?,
            isleaf: !relkind_has_partitions(relation.relkind),
            level,
        });
        if !relkind_has_partitions(relation.relkind) {
            continue;
        }
        for child in direct_partition_children(catalog, relation_oid)? {
            if !child.relispartition {
                continue;
            }
            queue.push_back((child.relation_oid, level + 1));
        }
    }

    Ok(entries)
}

fn child_partition_bound(child: &BoundRelation) -> Result<PartitionBoundSpec, ExecError> {
    let Some(bound) = child.relpartbound.as_deref() else {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition relation \"{}\" is missing relpartbound metadata",
                child.relation_oid
            ),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    deserialize_partition_bound(bound).map_err(ExecError::Parse)
}

pub(crate) fn partition_relation_is_default(child: &BoundRelation) -> Result<bool, ExecError> {
    Ok(child_partition_bound(child)?.is_default())
}

fn key_values(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let mut slot = TupleSlot::virtual_row(row.to_vec());
    spec.key_exprs
        .iter()
        .map(|expr| eval_expr(expr, &mut slot, ctx))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| match err {
            ExecError::DetailedError { .. } => err,
            other => ExecError::DetailedError {
                message: format!(
                    "failed to evaluate partition key for relation {}",
                    relation.relation_oid
                ),
                detail: Some(format!("{other:?}")),
                hint: None,
                sqlstate: "XX000",
            },
        })
}

fn partition_key_names(relation: &BoundRelation, spec: &LoweredPartitionSpec) -> Vec<String> {
    spec.partattrs
        .iter()
        .enumerate()
        .map(|(index, attnum)| {
            if *attnum == 0 {
                return spec
                    .key_sqls
                    .get(index)
                    .map(|expr| format_partition_key_expr_name(expr))
                    .unwrap_or_else(|| format!("partition key {}", index + 1));
            }
            relation
                .desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .map(|column| column.name.clone())
                .unwrap_or_else(|| format!("partition key {}", index + 1))
        })
        .collect()
}

fn format_partition_key_expr_name(expr_sql: &str) -> String {
    let stripped = strip_outer_expr_parens(expr_sql.trim());
    let normalized = normalize_partition_expr_operator_spacing(stripped);
    if normalized.contains(" + ")
        || normalized.contains(" - ")
        || normalized.contains(" * ")
        || normalized.contains(" / ")
        || normalized.contains(" % ")
    {
        format!("({normalized})")
    } else {
        normalized
    }
}

fn strip_outer_expr_parens(expr: &str) -> &str {
    if !expr.starts_with('(') || !expr.ends_with(')') {
        return expr;
    }
    let mut depth = 0_i32;
    for (index, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && index != expr.len() - 1 {
                    return expr;
                }
            }
            _ => {}
        }
    }
    expr[1..expr.len() - 1].trim()
}

fn normalize_partition_expr_operator_spacing(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars().peekable();
    while let Some(ch) = chars.next() {
        if matches!(ch, '+' | '*' | '/' | '%') || (ch == '-' && !out.trim_end().is_empty()) {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(' ');
            out.push(ch);
            out.push(' ');
            while chars.peek().is_some_and(|next| next.is_ascii_whitespace()) {
                chars.next();
            }
        } else {
            out.push(ch);
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn acl_item_grants_privilege(
    item: &str,
    effective_names: &BTreeSet<String>,
    privilege: char,
) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    let Some((privileges, _grantor)) = rest.split_once('/') else {
        return false;
    };
    effective_names.contains(grantee) && privileges.contains(privilege)
}

fn effective_acl_names(catalog: &dyn CatalogLookup, current_user_oid: u32) -> BTreeSet<String> {
    let mut names = BTreeSet::from([String::new()]);
    if let Some(role) = catalog
        .authid_rows()
        .into_iter()
        .find(|role| role.oid == current_user_oid)
    {
        names.insert(role.rolname);
    }
    names
}

fn column_acl_grants_select(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnum: i16,
    effective_names: &BTreeSet<String>,
) -> bool {
    catalog
        .attribute_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.attnum == attnum)
        .and_then(|row| row.attacl)
        .unwrap_or_default()
        .iter()
        .any(|item| acl_item_grants_privilege(item, effective_names, 'r'))
}

fn partition_key_detail_visible(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    ctx: &ExecutorContext,
) -> bool {
    if ctx.current_user_oid == BOOTSTRAP_SUPERUSER_OID || ctx.current_user_oid == relation.owner_oid
    {
        return true;
    }
    if spec.partattrs.iter().any(|attnum| *attnum == 0) {
        return false;
    }
    let effective_names = effective_acl_names(catalog, ctx.current_user_oid);
    if catalog
        .class_row_by_oid(relation.relation_oid)
        .and_then(|row| row.relacl)
        .unwrap_or_default()
        .iter()
        .any(|item| acl_item_grants_privilege(item, &effective_names, 'r'))
    {
        return true;
    }
    spec.partattrs.iter().all(|attnum| {
        column_acl_grants_select(catalog, relation.relation_oid, *attnum, &effective_names)
    })
}

fn render_partition_key_value(value: &Value, datetime_config: &DateTimeConfig) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::InternalChar(v) => (*v as char).to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Json(text) => text.to_string(),
        Value::JsonPath(text) => text.to_string(),
        Value::Xml(text) => text.to_string(),
        Value::Bytea(bytes) => format!("{bytes:?}"),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            render_datetime_value_text_with_config(value, datetime_config).unwrap_or_default()
        }
        other => format!("{other:?}"),
    }
}

fn render_partition_bound_value(
    value: &SerializedPartitionValue,
    datetime_config: &DateTimeConfig,
) -> String {
    render_partition_key_value(&partition_value_to_value(value), datetime_config)
}

fn render_partition_range_datum(
    value: &PartitionRangeDatumValue,
    datetime_config: &DateTimeConfig,
) -> String {
    match value {
        PartitionRangeDatumValue::MinValue => "MINVALUE".into(),
        PartitionRangeDatumValue::MaxValue => "MAXVALUE".into(),
        PartitionRangeDatumValue::Value(value) => {
            render_partition_bound_value(value, datetime_config)
        }
    }
}

pub(crate) fn render_partition_bound(
    bound: &PartitionBoundSpec,
    datetime_config: &DateTimeConfig,
) -> String {
    match bound {
        PartitionBoundSpec::List {
            is_default: true, ..
        }
        | PartitionBoundSpec::Range {
            is_default: true, ..
        } => "DEFAULT".into(),
        PartitionBoundSpec::List { values, .. } => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(|value| render_partition_bound_value(value, datetime_config))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Range { from, to, .. } => format!(
            "FOR VALUES FROM ({}) TO ({})",
            from.iter()
                .map(|value| render_partition_range_datum(value, datetime_config))
                .collect::<Vec<_>>()
                .join(", "),
            to.iter()
                .map(|value| render_partition_range_datum(value, datetime_config))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (modulus {modulus}, remainder {remainder})")
        }
    }
}

pub(crate) fn render_partition_keydef(
    relation: &BoundRelation,
) -> Result<Option<String>, ExecError> {
    if relation.relkind != 'p' {
        return Ok(None);
    }
    let spec = relation_partition_spec(relation).map_err(ExecError::Parse)?;
    let strategy = match spec.strategy {
        PartitionStrategy::List => "LIST",
        PartitionStrategy::Range => "RANGE",
        PartitionStrategy::Hash => "HASH",
    };
    Ok(Some(format!(
        "{strategy} ({})",
        partition_key_names(relation, &spec).join(", ")
    )))
}

fn no_partition_detail(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Option<String>, ExecError> {
    if !partition_key_detail_visible(catalog, relation, spec, ctx) {
        return Ok(None);
    }
    let names = partition_key_names(relation, spec).join(", ");
    let values = key_values(relation, spec, row, ctx)?
        .iter()
        .map(|value| render_partition_key_value(value, &ctx.datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Some(format!(
        "Partition key of the failing row contains ({names}) = ({values})."
    )))
}

fn partition_constraint_violation(
    relation_name: &str,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("new row for relation \"{relation_name}\" violates partition constraint"),
        detail: Some(format_failing_row_detail(row, datetime_config)),
        hint: None,
        sqlstate: "23514",
    }
}

fn attach_partition_constraint_violation(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "partition constraint of relation \"{relation_name}\" is violated by some row"
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    }
}

fn default_partition_constraint_violation(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "updated partition constraint for default partition \"{relation_name}\" would be violated by some row"
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    }
}

fn no_partition_for_row(relation_name: &str, detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: format!("no partition of relation \"{relation_name}\" found for row"),
        detail,
        hint: None,
        sqlstate: "23514",
    }
}

fn compare_serialized_values(
    left: &SerializedPartitionValue,
    right: &SerializedPartitionValue,
    collation_oid: Option<u32>,
) -> Result<Ordering, ExecError> {
    compare_order_values(
        &partition_value_to_value(left),
        &partition_value_to_value(right),
        collation_oid,
        None,
        false,
    )
}

fn compare_partition_key_to_bound(
    key_values: &[Value],
    bound: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Result<Ordering, ExecError> {
    for ((key, datum), collation_oid) in key_values
        .iter()
        .zip(bound.iter())
        .zip(collations.iter().copied())
    {
        let cmp = match datum {
            PartitionRangeDatumValue::MinValue => Ordering::Greater,
            PartitionRangeDatumValue::MaxValue => Ordering::Less,
            PartitionRangeDatumValue::Value(value) => compare_order_values(
                key,
                &partition_value_to_value(value),
                (collation_oid != 0).then_some(collation_oid),
                None,
                false,
            )?,
        };
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_range_bounds(
    left: &[PartitionRangeDatumValue],
    right: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Result<Ordering, ExecError> {
    for ((left, right), collation_oid) in left
        .iter()
        .zip(right.iter())
        .zip(collations.iter().copied())
    {
        let cmp = match (left, right) {
            (PartitionRangeDatumValue::MinValue, PartitionRangeDatumValue::MinValue)
            | (PartitionRangeDatumValue::MaxValue, PartitionRangeDatumValue::MaxValue) => {
                Ordering::Equal
            }
            (PartitionRangeDatumValue::MinValue, _) => Ordering::Less,
            (_, PartitionRangeDatumValue::MinValue) => Ordering::Greater,
            (PartitionRangeDatumValue::MaxValue, _) => Ordering::Greater,
            (_, PartitionRangeDatumValue::MaxValue) => Ordering::Less,
            (PartitionRangeDatumValue::Value(left), PartitionRangeDatumValue::Value(right)) => {
                compare_serialized_values(
                    left,
                    right,
                    (collation_oid != 0).then_some(collation_oid),
                )?
            }
        };
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn partition_hash_value(
    values: &[Value],
    spec: &LoweredPartitionSpec,
    ctx: &mut ExecutorContext,
) -> Result<u64, ExecError> {
    let mut row_hash = 0_u64;
    for (index, value) in values.iter().enumerate() {
        let opclass = spec.partclass.get(index).copied();
        let value_hash = if let Some(proc_oid) = hash_support_proc(index, spec, ctx) {
            execute_partition_hash_support_proc(proc_oid, value, ctx)?
        } else {
            crate::backend::access::hash::hash_value_extended(
                value,
                opclass,
                crate::backend::access::hash::HASH_PARTITION_SEED,
            )
            .map_err(unsupported_hash_key_error)?
        };
        if let Some(value_hash) = value_hash {
            row_hash = crate::backend::access::hash::hash_combine64(row_hash, value_hash);
        }
    }
    Ok(row_hash)
}

fn unsupported_hash_key_error(message: String) -> ExecError {
    ExecError::DetailedError {
        message: format!("unsupported hash partition key value {message}"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn hash_support_proc(
    key_index: usize,
    spec: &LoweredPartitionSpec,
    ctx: &ExecutorContext,
) -> Option<u32> {
    let catalog = ctx.catalog.as_deref()?;
    let opclass_oid = *spec.partclass.get(key_index)?;
    let opclass = catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    let key_type = *spec.key_types.get(key_index)?;
    let key_type_oid = crate::backend::utils::cache::catcache::sql_type_oid(key_type);
    catalog
        .amproc_rows()
        .into_iter()
        .find(|row| {
            row.amprocfamily == opclass.opcfamily
                && row.amprocnum == 2
                && hash_amproc_type_matches(row.amproclefttype, key_type_oid, key_type)
                && hash_amproc_type_matches(row.amprocrighttype, key_type_oid, key_type)
        })
        .map(|row| row.amproc)
}

fn hash_amproc_type_matches(proc_type_oid: u32, key_type_oid: u32, key_type: SqlType) -> bool {
    proc_type_oid == key_type_oid
        || proc_type_oid == ANYOID
        || (key_type.is_array && proc_type_oid == ANYARRAYOID)
}

fn execute_partition_hash_support_proc(
    proc_oid: u32,
    value: &Value,
    ctx: &mut ExecutorContext,
) -> Result<Option<u64>, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let result = execute_scalar_function_value_call(
        proc_oid,
        &[
            value.clone(),
            Value::Int64(crate::backend::access::hash::HASH_PARTITION_SEED as i64),
        ],
        ctx,
    )?;
    match result {
        Value::Null => Ok(None),
        Value::Int64(value) => Ok(Some(value as u64)),
        Value::Int32(value) => Ok(Some(value as u64)),
        Value::Int16(value) => Ok(Some(value as u64)),
        other => Err(ExecError::DetailedError {
            message: "hash partition support function returned non-integer value".into(),
            detail: Some(format!("returned {other:?}")),
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn row_matches_explicit_bound(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    let keys = key_values(relation, spec, row, ctx)?;
    row_matches_explicit_bound_with_keys(spec, bound, &keys, ctx)
}

fn row_matches_explicit_bound_with_keys(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    keys: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    match bound {
        PartitionBoundSpec::List { values, .. } => {
            let key = keys.first().unwrap_or(&Value::Null);
            values.iter().try_fold(false, |matched, value| {
                if matched {
                    return Ok(true);
                }
                Ok(compare_order_values(
                    key,
                    &partition_value_to_value(value),
                    spec.partcollation.first().copied().filter(|oid| *oid != 0),
                    None,
                    false,
                )? == Ordering::Equal)
            })
        }
        PartitionBoundSpec::Range { from, to, .. } => {
            if keys.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(false);
            }
            Ok(
                compare_partition_key_to_bound(keys, from, &spec.partcollation)? != Ordering::Less
                    && compare_partition_key_to_bound(keys, to, &spec.partcollation)?
                        == Ordering::Less,
            )
        }
        PartitionBoundSpec::Hash { modulus, remainder } => {
            Ok(partition_hash_value(keys, spec, ctx)? % (*modulus as u64) == *remainder as u64)
        }
    }
}

fn get_partition_for_tuple(
    dispatch: &mut PartitionDispatch,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Option<usize>, ExecError> {
    let keys = key_values(&dispatch.reldesc, &dispatch.key, row, ctx)?;
    if matches!(
        dispatch.key.strategy,
        PartitionStrategy::List | PartitionStrategy::Range
    ) && let Some(part_index) = dispatch.partdesc.cached_partition_index()
    {
        let child = &dispatch.partdesc.children[part_index];
        if row_matches_explicit_bound_with_keys(&dispatch.key, &child.bound, &keys, ctx)? {
            return Ok(Some(part_index));
        }
    }

    let mut matched_index = None;
    for &part_index in &dispatch.partdesc.boundinfo.indexes {
        let child = &dispatch.partdesc.children[part_index];
        if row_matches_explicit_bound_with_keys(&dispatch.key, &child.bound, &keys, ctx)? {
            matched_index = Some(part_index);
            break;
        }
    }
    if let Some(part_index) = matched_index {
        dispatch.partdesc.record_partition_match(part_index);
        return Ok(Some(part_index));
    }

    Ok(dispatch.partdesc.boundinfo.default_index)
}

fn find_explicit_partition_match(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    row: &[Value],
    skip_child_oid: Option<u32>,
    ctx: &mut ExecutorContext,
) -> Result<Option<BoundRelation>, ExecError> {
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        let bound = child_partition_bound(&child)?;
        if bound.is_default() {
            continue;
        }
        if row_matches_explicit_bound(parent, &spec, &bound, row, ctx)? {
            return Ok(Some(child.clone()));
        }
    }
    Ok(None)
}

fn default_partition(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    skip_child_oid: Option<u32>,
) -> Result<Option<BoundRelation>, ExecError> {
    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        if child_partition_bound(&child)?.is_default() {
            return Ok(Some(child.clone()));
        }
    }
    Ok(None)
}

fn candidate_row_matches_partition(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    bound: &PartitionBoundSpec,
    row: &[Value],
    skip_child_oid: Option<u32>,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    if bound.is_default() {
        return Ok(
            find_explicit_partition_match(catalog, parent, row, skip_child_oid, ctx)?.is_none(),
        );
    }
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    row_matches_explicit_bound(parent, &spec, bound, row, ctx)
}

fn bounds_overlap(
    spec: &LoweredPartitionSpec,
    left: &PartitionBoundSpec,
    right: &PartitionBoundSpec,
) -> Result<bool, ExecError> {
    match (left, right) {
        (
            PartitionBoundSpec::List { values: left, .. },
            PartitionBoundSpec::List { values: right, .. },
        ) => {
            for left in left {
                for right in right {
                    if compare_serialized_values(
                        left,
                        right,
                        spec.partcollation.first().copied().filter(|oid| *oid != 0),
                    )? == Ordering::Equal
                    {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        (
            PartitionBoundSpec::Range {
                from: left_from,
                to: left_to,
                ..
            },
            PartitionBoundSpec::Range {
                from: right_from,
                to: right_to,
                ..
            },
        ) => Ok(
            compare_range_bounds(left_to, right_from, &spec.partcollation)? == Ordering::Greater
                && compare_range_bounds(right_to, left_from, &spec.partcollation)?
                    == Ordering::Greater,
        ),
        (
            PartitionBoundSpec::Hash {
                modulus: left_modulus,
                remainder: left_remainder,
            },
            PartitionBoundSpec::Hash {
                modulus: right_modulus,
                remainder: right_remainder,
            },
        ) => Ok(hash_bounds_overlap(
            *left_modulus,
            *left_remainder,
            *right_modulus,
            *right_remainder,
        )),
        _ => Ok(false),
    }
}

fn hash_moduli_compatible(left: i32, right: i32) -> bool {
    let lower = left.min(right);
    let higher = left.max(right);
    higher % lower == 0
}

fn hash_modulus_compatibility_detail(
    new_modulus: i32,
    existing_modulus: i32,
    existing_name: &str,
) -> String {
    if new_modulus > existing_modulus {
        format!(
            "The new modulus {new_modulus} is not divisible by {existing_modulus}, the modulus of existing partition \"{existing_name}\"."
        )
    } else {
        format!(
            "The new modulus {new_modulus} is not a factor of {existing_modulus}, the modulus of existing partition \"{existing_name}\"."
        )
    }
}

fn hash_bounds_overlap(
    left_modulus: i32,
    left_remainder: i32,
    right_modulus: i32,
    right_remainder: i32,
) -> bool {
    if !hash_moduli_compatible(left_modulus, right_modulus) {
        return false;
    }
    if left_modulus <= right_modulus {
        right_remainder % left_modulus == left_remainder
    } else {
        left_remainder % right_modulus == right_remainder
    }
}

pub(crate) fn validate_new_partition_bound(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    new_relation_name: &str,
    bound: &PartitionBoundSpec,
    skip_child_oid: Option<u32>,
) -> Result<(), ExecError> {
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    if let PartitionBoundSpec::Range {
        from,
        to,
        is_default: false,
    } = bound
        && compare_range_bounds(from, to, &spec.partcollation)? != Ordering::Less
    {
        return Err(ExecError::DetailedError {
            message: format!("empty range bound specified for partition \"{new_relation_name}\""),
            detail: Some(format!(
                "Specified lower bound ({}) is greater than or equal to upper bound ({}).",
                format_range_bound_for_error(from),
                format_range_bound_for_error(to)
            )),
            hint: None,
            sqlstate: "42P17",
        });
    }
    if bound.is_default()
        && let Some(existing) = default_partition(catalog, parent, skip_child_oid)?
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{new_relation_name}\" conflicts with existing default partition \"{}\"",
                relation_name_for_oid(catalog, existing.relation_oid)
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }

    if let PartitionBoundSpec::Hash {
        modulus: new_modulus,
        remainder: new_remainder,
    } = bound
    {
        let mut next_larger_incompatible: Option<(i32, String)> = None;
        let mut matching_lower_incompatible: Option<(i32, String)> = None;
        let mut first_lower_incompatible: Option<(i32, String)> = None;
        for child in direct_partition_children(catalog, parent.relation_oid)? {
            if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
                continue;
            }
            let existing_bound = child_partition_bound(&child)?;
            if let PartitionBoundSpec::Hash {
                modulus: existing_modulus,
                remainder: existing_remainder,
            } = existing_bound
                && !hash_moduli_compatible(*new_modulus, existing_modulus)
            {
                let existing_name = relation_name_for_oid(catalog, child.relation_oid);
                if existing_modulus > *new_modulus {
                    if next_larger_incompatible
                        .as_ref()
                        .is_none_or(|(modulus, _)| existing_modulus < *modulus)
                    {
                        next_larger_incompatible = Some((existing_modulus, existing_name));
                    }
                } else if *new_remainder % existing_modulus == existing_remainder {
                    matching_lower_incompatible = Some((existing_modulus, existing_name));
                } else if first_lower_incompatible.is_none() {
                    first_lower_incompatible = Some((existing_modulus, existing_name));
                }
            }
        }
        if let Some((existing_modulus, existing_name)) = next_larger_incompatible
            .or(matching_lower_incompatible)
            .or(first_lower_incompatible)
        {
            return Err(ExecError::DetailedError {
                message: "every hash partition modulus must be a factor of the next larger modulus"
                    .into(),
                detail: Some(hash_modulus_compatibility_detail(
                    *new_modulus,
                    existing_modulus,
                    &existing_name,
                )),
                hint: None,
                sqlstate: "42P17",
            });
        }
    }

    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        let existing_bound = child_partition_bound(&child)?;
        if existing_bound.is_default() {
            continue;
        }
        if bounds_overlap(&spec, bound, &existing_bound)? {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition \"{new_relation_name}\" would overlap partition \"{}\"",
                    relation_name_for_oid(catalog, child.relation_oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P17",
            });
        }
    }
    Ok(())
}

fn format_range_bound_for_error(bound: &[PartitionRangeDatumValue]) -> String {
    bound
        .iter()
        .map(|datum| match datum {
            PartitionRangeDatumValue::MinValue => "MINVALUE".into(),
            PartitionRangeDatumValue::MaxValue => "MAXVALUE".into(),
            PartitionRangeDatumValue::Value(value) => {
                let value = partition_value_to_value(value);
                render_partition_key_value(
                    &value,
                    &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn column_attnum_by_name(relation: &BoundRelation, column_name: &str) -> Option<i16> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                .then_some(index.saturating_add(1) as i16)
        })
}

fn column_name_for_attnum(relation: &BoundRelation, attnum: i16) -> Option<&str> {
    relation
        .desc
        .columns
        .get(attnum.saturating_sub(1) as usize)
        .filter(|column| !column.dropped)
        .map(|column| column.name.as_str())
}

fn not_null_constraint_for_attnum(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnum: i16,
) -> Option<PgConstraintRow> {
    catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| {
            row.contype == CONSTRAINT_NOTNULL
                && row
                    .conkey
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&attnum))
        })
}

fn validate_attach_check_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), ExecError> {
    let child_constraints = catalog
        .constraint_rows_for_relation(child.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_CHECK)
        .collect::<Vec<_>>();
    for parent_constraint in catalog
        .constraint_rows_for_relation(parent.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_CHECK && !row.connoinherit)
    {
        let Some(child_constraint) = child_constraints
            .iter()
            .find(|row| row.conname.eq_ignore_ascii_case(&parent_constraint.conname))
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "child table is missing constraint \"{}\"",
                    parent_constraint.conname
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        if child_constraint.conbin != parent_constraint.conbin {
            return Err(ExecError::DetailedError {
                message: format!(
                    "child table \"{child_name}\" has different definition for check constraint \"{}\"",
                    parent_constraint.conname
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        validate_attach_constraint_merge_state(&parent_constraint, child_constraint, child_name)?;
    }
    Ok(())
}

fn validate_attach_not_null_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), ExecError> {
    for parent_constraint in catalog
        .constraint_rows_for_relation(parent.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_NOTNULL && !row.connoinherit)
    {
        let Some(parent_attnum) = parent_constraint
            .conkey
            .as_ref()
            .and_then(|keys| keys.first())
            .copied()
        else {
            continue;
        };
        let Some(column_name) = column_name_for_attnum(parent, parent_attnum) else {
            continue;
        };
        let Some(child_attnum) = column_attnum_by_name(child, column_name) else {
            continue;
        };
        let Some(child_constraint) =
            not_null_constraint_for_attnum(catalog, child.relation_oid, child_attnum)
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{column_name}\" in child table \"{child_name}\" must be marked NOT NULL"
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        validate_attach_constraint_merge_state(&parent_constraint, &child_constraint, child_name)?;
    }
    Ok(())
}

pub(crate) fn validate_attach_partition_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
) -> Result<(), ExecError> {
    let child_name = relation_name_for_oid(catalog, child.relation_oid);
    validate_attach_check_constraints(catalog, parent, child, &child_name)?;
    validate_attach_not_null_constraints(catalog, parent, child, &child_name)?;
    Ok(())
}

fn validate_attach_constraint_merge_state(
    parent_constraint: &PgConstraintRow,
    child_constraint: &PgConstraintRow,
    child_name: &str,
) -> Result<(), ExecError> {
    if child_constraint.connoinherit {
        return Err(ExecError::DetailedError {
            message: format!(
                "constraint \"{}\" conflicts with non-inherited constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if parent_constraint.convalidated
        && child_constraint.conenforced
        && !child_constraint.convalidated
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if parent_constraint.conenforced && !child_constraint.conenforced {
        return Err(ExecError::DetailedError {
            message: format!(
                "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

pub(crate) fn validate_partition_relation_compatibility(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    _parent_name: &str,
    child: &BoundRelation,
    _child_name: &str,
) -> Result<(), ExecError> {
    let parent_name = relation_name_for_oid(catalog, parent.relation_oid);
    let child_name = relation_name_for_oid(catalog, child.relation_oid);
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        return Err(ExecError::DetailedError {
            message: format!(
                "ALTER action ATTACH PARTITION cannot be performed on relation \"{parent_name}\""
            ),
            detail: Some("This operation is not supported for tables.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    if !matches!(child.relkind, 'r' | 'p' | 'f') {
        return Err(ExecError::Parse(
            crate::backend::parser::ParseError::WrongObjectType {
                name: child_name.to_string(),
                expected: "table",
            },
        ));
    }
    if child.relispartition {
        return Err(ExecError::DetailedError {
            message: format!("\"{child_name}\" is already a partition"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if !catalog.inheritance_parents(child.relation_oid).is_empty() {
        return Err(ExecError::DetailedError {
            message: "cannot attach inheritance child as partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if matches!(child.relkind, 'r' | 'f')
        && !catalog.inheritance_children(child.relation_oid).is_empty()
    {
        return Err(ExecError::DetailedError {
            message: "cannot attach inheritance parent as partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if child.relpersistence != parent.relpersistence {
        let child_persistence = if child.relpersistence == 't' {
            "temporary"
        } else {
            "permanent"
        };
        let parent_persistence = if parent.relpersistence == 't' {
            "temporary"
        } else {
            "permanent"
        };
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot attach a {child_persistence} relation as partition of {parent_persistence} relation \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let parent_columns = parent
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    let child_columns = child
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    for child_column in &child_columns {
        if parent_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(&child_column.name))
        {
            continue;
        }
        return Err(ExecError::DetailedError {
            message: format!(
                "table \"{child_name}\" contains column \"{}\" not found in parent \"{parent_name}\"",
                child_column.name
            ),
            detail: Some(
                "The new partition may contain only the columns present in parent.".into(),
            ),
            hint: None,
            sqlstate: "42804",
        });
    }
    for parent_column in &parent_columns {
        let Some(child_column) = child_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&parent_column.name))
        else {
            return Err(ExecError::DetailedError {
                message: format!("child table is missing column \"{}\"", parent_column.name),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        if parent_column.sql_type != child_column.sql_type {
            return Err(ExecError::DetailedError {
                message: format!(
                    "child table \"{child_name}\" has different type for column \"{}\"",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        if parent_column.collation_oid != child_column.collation_oid {
            return Err(ExecError::DetailedError {
                message: format!(
                    "child table \"{child_name}\" has different collation for column \"{}\"",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P21",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_default_partition_rows_for_new_bound(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    bound: &PartitionBoundSpec,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if bound.is_default() {
        return Ok(());
    }
    let Some(default_partition) = default_partition(catalog, parent, None)? else {
        return Ok(());
    };
    let relation_oids = if default_partition.relkind == 'p' {
        catalog
            .find_all_inheritors(default_partition.relation_oid)
            .into_iter()
            .filter(|oid| {
                catalog
                    .relation_by_oid(*oid)
                    .is_some_and(|relation| relation.relkind == 'r')
            })
            .collect::<Vec<_>>()
    } else {
        vec![default_partition.relation_oid]
    };
    for relation_oid in relation_oids {
        let Some(relation) = catalog.relation_by_oid(relation_oid) else {
            continue;
        };
        for (_, row) in
            collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?
        {
            let parent_row =
                remap_partition_row_to_parent_layout(&row, &relation.desc, &parent.desc)?;
            if candidate_row_matches_partition(catalog, parent, bound, &parent_row, None, ctx)? {
                return Err(default_partition_constraint_violation(
                    &relation_name_for_oid(catalog, relation_oid),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_relation_rows_for_partition_bound(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    bound: &PartitionBoundSpec,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let relation_oids = if child.relkind == 'p' {
        catalog
            .find_all_inheritors(child.relation_oid)
            .into_iter()
            .filter(|oid| {
                catalog
                    .relation_by_oid(*oid)
                    .is_some_and(|relation| relation.relkind == 'r')
            })
            .collect::<Vec<_>>()
    } else {
        vec![child.relation_oid]
    };

    for relation_oid in relation_oids {
        let Some(relation) = catalog.relation_by_oid(relation_oid) else {
            continue;
        };
        for (_, row) in
            collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?
        {
            let parent_row =
                remap_partition_row_to_parent_layout(&row, &relation.desc, &parent.desc)?;
            if !candidate_row_matches_partition(catalog, parent, bound, &parent_row, None, ctx)? {
                return Err(attach_partition_constraint_violation(
                    &relation_name_for_oid(catalog, relation_oid),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn route_partition_target(
    catalog: &dyn CatalogLookup,
    target: &BoundRelation,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<BoundRelation, ExecError> {
    let mut proute = exec_setup_partition_tuple_routing(catalog, target)?;
    exec_find_partition(catalog, &mut proute, target, row, ctx)
}

pub(crate) fn exec_find_partition(
    catalog: &dyn CatalogLookup,
    proute: &mut PartitionTupleRouting,
    target: &BoundRelation,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<BoundRelation, ExecError> {
    if target.relispartition {
        let target_name = relation_name_for_oid(catalog, target.relation_oid);
        let mut child = target.clone();
        let mut current_row = row.to_vec();
        while let Some(parent) = declarative_parent(catalog, &child)? {
            let parent_row =
                remap_partition_row_to_parent_layout(&current_row, &child.desc, &parent.desc)?;
            let selected = find_partition_child(catalog, proute, &parent, &parent_row, ctx)?
                .map(|lookup| lookup.reldesc);
            if selected
                .as_ref()
                .is_none_or(|relation| relation.relation_oid != child.relation_oid)
            {
                return Err(partition_constraint_violation(
                    &target_name,
                    row,
                    &ctx.datetime_config,
                ));
            }
            current_row = parent_row;
            child = parent;
        }
    }

    let mut current = target.clone();
    let mut current_row = row.to_vec();
    loop {
        if current.relkind != 'p' {
            return Ok(current);
        }

        let Some(selected) = find_partition_child(catalog, proute, &current, &current_row, ctx)?
        else {
            let dispatch = proute.dispatch_info_for_relation(catalog, &current)?;
            return Err(no_partition_for_row(
                &relation_name_for_oid(catalog, current.relation_oid),
                no_partition_detail(catalog, &current, &dispatch.key, &current_row, ctx)?,
            ));
        };
        if selected.is_leaf {
            return Ok(selected.reldesc);
        }
        current_row = remap_partition_row_to_child_layout(
            &current_row,
            &current.desc,
            &selected.reldesc.desc,
        )?;
        current = selected.reldesc;
    }
}

pub(crate) fn remap_partition_row_to_child_layout(
    row: &[Value],
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
) -> Result<Vec<Value>, ExecError> {
    let mut child_row = vec![Value::Null; child_desc.columns.len()];
    for (child_idx, child_column) in child_desc.columns.iter().enumerate() {
        if child_column.dropped {
            continue;
        }
        let Some((parent_idx, parent_column)) =
            parent_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, parent_column)| {
                    !parent_column.dropped
                        && parent_column.name.eq_ignore_ascii_case(&child_column.name)
                })
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition column \"{}\" is missing from partitioned table",
                    child_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        };
        if parent_column.sql_type != child_column.sql_type {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition column \"{}\" has different type than partitioned table",
                    child_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        child_row[child_idx] = row.get(parent_idx).cloned().unwrap_or(Value::Null);
    }
    Ok(child_row)
}

pub(crate) fn remap_partition_row_to_parent_layout(
    row: &[Value],
    child_desc: &RelationDesc,
    parent_desc: &RelationDesc,
) -> Result<Vec<Value>, ExecError> {
    let mut parent_row = vec![Value::Null; parent_desc.columns.len()];
    for (parent_idx, parent_column) in parent_desc.columns.iter().enumerate() {
        if parent_column.dropped {
            continue;
        }
        let Some((child_idx, child_column)) =
            child_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, child_column)| {
                    !child_column.dropped
                        && child_column.name.eq_ignore_ascii_case(&parent_column.name)
                })
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition column \"{}\" is missing from partitioned table",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        };
        if child_column.sql_type != parent_column.sql_type {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition column \"{}\" has different type than partitioned table",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        parent_row[parent_idx] = row.get(child_idx).cloned().unwrap_or(Value::Null);
    }
    Ok(parent_row)
}

struct PartitionLookup {
    reldesc: BoundRelation,
    is_leaf: bool,
}

fn find_partition_child(
    catalog: &dyn CatalogLookup,
    proute: &mut PartitionTupleRouting,
    relation: &BoundRelation,
    row: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Option<PartitionLookup>, ExecError> {
    let dispatch = proute.dispatch_info_for_relation(catalog, relation)?;
    let Some(part_index) = get_partition_for_tuple(dispatch, row, ctx)? else {
        return Ok(None);
    };
    let child = &dispatch.partdesc.children[part_index];
    Ok(Some(PartitionLookup {
        reldesc: child.reldesc.clone(),
        is_leaf: child.is_leaf,
    }))
}
