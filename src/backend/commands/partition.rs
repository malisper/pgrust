use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::value_io::format_failing_row_detail;
use crate::backend::executor::{
    ExecError, ExecutorContext, compare_order_values, render_datetime_value_text_with_config,
};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec,
    PartitionRangeDatumValue, PartitionStrategy, SerializedPartitionValue,
    deserialize_partition_bound, partition_value_to_value, relation_partition_spec,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::catalog::BPCHAR_HASH_OPCLASS_OID;
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

fn relation_can_participate_in_partition_tree(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> bool {
    catalog
        .relation_by_oid(relation_oid)
        .is_some_and(|relation| relation.relispartition || relation.partitioned_table.is_some())
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
                .filter(|parent| parent.partitioned_table.is_some())
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
            isleaf: relation.partitioned_table.is_none(),
            level,
        });
        if relation.partitioned_table.is_none() {
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

fn key_values(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
) -> Result<Vec<Value>, ExecError> {
    spec.partattrs
        .iter()
        .map(|attnum| {
            row.get(attnum.saturating_sub(1) as usize)
                .cloned()
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "partition key attribute {} is missing from row for relation {}",
                        attnum, relation.relation_oid
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

fn partition_key_names(relation: &BoundRelation, spec: &LoweredPartitionSpec) -> Vec<String> {
    spec.partattrs
        .iter()
        .filter_map(|attnum| relation.desc.columns.get(attnum.saturating_sub(1) as usize))
        .map(|column| column.name.clone())
        .collect()
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

fn no_partition_detail(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let names = partition_key_names(relation, spec).join(", ");
    let values = key_values(relation, spec, row)?
        .iter()
        .map(|value| render_partition_key_value(value, datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "Partition key of the failing row contains ({names}) = ({values})."
    ))
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

fn no_partition_for_row(relation_name: &str, detail: String) -> ExecError {
    ExecError::DetailedError {
        message: format!("no partition of relation \"{relation_name}\" found for row"),
        detail: Some(detail),
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

const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;
const HASH_INITIAL_VALUE: u32 = 0x9e37_79b9 + 3_923_095;

fn hash_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}

fn hash_final(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}

fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64 {
    let mut a = HASH_INITIAL_VALUE.wrapping_add(bytes.len() as u32);
    let mut b = a;
    let mut c = a;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = hash_mix(a, b, c);
    }

    let mut chunks = bytes;
    while chunks.len() >= 12 {
        a = a.wrapping_add(u32::from_le_bytes(chunks[0..4].try_into().unwrap()));
        b = b.wrapping_add(u32::from_le_bytes(chunks[4..8].try_into().unwrap()));
        c = c.wrapping_add(u32::from_le_bytes(chunks[8..12].try_into().unwrap()));
        (a, b, c) = hash_mix(a, b, c);
        chunks = &chunks[12..];
    }

    if chunks.len() >= 11 {
        c = c.wrapping_add((chunks[10] as u32) << 24);
    }
    if chunks.len() >= 10 {
        c = c.wrapping_add((chunks[9] as u32) << 16);
    }
    if chunks.len() >= 9 {
        c = c.wrapping_add((chunks[8] as u32) << 8);
    }
    if chunks.len() >= 8 {
        b = b.wrapping_add((chunks[7] as u32) << 24);
    }
    if chunks.len() >= 7 {
        b = b.wrapping_add((chunks[6] as u32) << 16);
    }
    if chunks.len() >= 6 {
        b = b.wrapping_add((chunks[5] as u32) << 8);
    }
    if chunks.len() >= 5 {
        b = b.wrapping_add(chunks[4] as u32);
    }
    if chunks.len() >= 4 {
        a = a.wrapping_add((chunks[3] as u32) << 24);
    }
    if chunks.len() >= 3 {
        a = a.wrapping_add((chunks[2] as u32) << 16);
    }
    if chunks.len() >= 2 {
        a = a.wrapping_add((chunks[1] as u32) << 8);
    }
    if !chunks.is_empty() {
        a = a.wrapping_add(chunks[0] as u32);
    }

    (_, b, c) = hash_final(a, b, c);
    ((b as u64) << 32) | c as u64
}

fn hash_uint32_extended(value: u32, seed: u64) -> u64 {
    let mut a = HASH_INITIAL_VALUE.wrapping_add(std::mem::size_of::<u32>() as u32);
    let mut b = a;
    let mut c = a;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = hash_mix(a, b, c);
    }

    a = a.wrapping_add(value);
    (_, b, c) = hash_final(a, b, c);
    ((b as u64) << 32) | c as u64
}

fn hash_int8_extended(value: i64, seed: u64) -> u64 {
    let mut lohalf = value as u32;
    let hihalf = (value >> 32) as u32;
    lohalf ^= if value >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(lohalf, seed)
}

fn hash_combine64(mut left: u64, right: u64) -> u64 {
    left ^= right
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(left << 54)
        .wrapping_add(left >> 7);
    left
}

fn hash_partition_value(value: &Value, opclass: Option<u32>) -> Result<Option<u64>, ExecError> {
    let hash = match value {
        Value::Null => return Ok(None),
        Value::Bool(value) => hash_uint32_extended(u32::from(*value), HASH_PARTITION_SEED),
        Value::InternalChar(value) => {
            hash_uint32_extended(i32::from(*value) as u32, HASH_PARTITION_SEED)
        }
        Value::Int16(value) => hash_uint32_extended(i32::from(*value) as u32, HASH_PARTITION_SEED),
        Value::Int32(value) => hash_uint32_extended(*value as u32, HASH_PARTITION_SEED),
        Value::Int64(value) => hash_int8_extended(*value, HASH_PARTITION_SEED),
        Value::Date(value) => hash_uint32_extended(value.0 as u32, HASH_PARTITION_SEED),
        Value::Time(value) => hash_int8_extended(value.0, HASH_PARTITION_SEED),
        Value::Timestamp(value) => hash_int8_extended(value.0, HASH_PARTITION_SEED),
        Value::TimestampTz(value) => hash_int8_extended(value.0, HASH_PARTITION_SEED),
        Value::TimeTz(value) => {
            let mut bytes = Vec::with_capacity(12);
            bytes.extend_from_slice(&value.time.0.to_le_bytes());
            bytes.extend_from_slice(&value.offset_seconds.to_le_bytes());
            hash_bytes_extended(&bytes, HASH_PARTITION_SEED)
        }
        Value::Float64(value) if *value == 0.0 => HASH_PARTITION_SEED,
        Value::Float64(value) if value.is_nan() => {
            hash_bytes_extended(&f64::NAN.to_le_bytes(), HASH_PARTITION_SEED)
        }
        Value::Float64(value) => hash_bytes_extended(&value.to_le_bytes(), HASH_PARTITION_SEED),
        Value::Numeric(value) => {
            hash_bytes_extended(value.render().as_bytes(), HASH_PARTITION_SEED)
        }
        Value::Bytea(value) => hash_bytes_extended(value, HASH_PARTITION_SEED),
        value if value.as_text().is_some() => {
            let mut text = value.as_text().unwrap();
            if opclass == Some(BPCHAR_HASH_OPCLASS_OID) {
                text = text.trim_end_matches(' ');
            }
            hash_bytes_extended(text.as_bytes(), HASH_PARTITION_SEED)
        }
        other => {
            return Err(ExecError::DetailedError {
                message: format!("unsupported hash partition key value {other:?}"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
    };
    Ok(Some(hash))
}

fn partition_hash_value(values: &[Value], opclasses: &[u32]) -> Result<u64, ExecError> {
    let mut row_hash = 0_u64;
    for (index, value) in values.iter().enumerate() {
        if let Some(value_hash) = hash_partition_value(value, opclasses.get(index).copied())? {
            row_hash = hash_combine64(row_hash, value_hash);
        }
    }
    Ok(row_hash)
}

fn row_matches_explicit_bound(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    row: &[Value],
) -> Result<bool, ExecError> {
    let keys = key_values(relation, spec, row)?;
    row_matches_explicit_bound_with_keys(spec, bound, &keys)
}

fn row_matches_explicit_bound_with_keys(
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    keys: &[Value],
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
        PartitionBoundSpec::Hash { modulus, remainder } => Ok(partition_hash_value(
            keys,
            &spec.partclass,
        )? % (*modulus as u64)
            == *remainder as u64),
    }
}

fn get_partition_for_tuple(
    dispatch: &mut PartitionDispatch,
    row: &[Value],
) -> Result<Option<usize>, ExecError> {
    let keys = key_values(&dispatch.reldesc, &dispatch.key, row)?;
    if matches!(
        dispatch.key.strategy,
        PartitionStrategy::List | PartitionStrategy::Range
    ) && let Some(part_index) = dispatch.partdesc.cached_partition_index()
    {
        let child = &dispatch.partdesc.children[part_index];
        if row_matches_explicit_bound_with_keys(&dispatch.key, &child.bound, &keys)? {
            return Ok(Some(part_index));
        }
    }

    let mut matched_index = None;
    for &part_index in &dispatch.partdesc.boundinfo.indexes {
        let child = &dispatch.partdesc.children[part_index];
        if row_matches_explicit_bound_with_keys(&dispatch.key, &child.bound, &keys)? {
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
        if row_matches_explicit_bound(parent, &spec, &bound, row)? {
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
) -> Result<bool, ExecError> {
    if bound.is_default() {
        return Ok(find_explicit_partition_match(catalog, parent, row, skip_child_oid)?.is_none());
    }
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    row_matches_explicit_bound(parent, &spec, bound, row)
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
        ..
    } = bound
    {
        for child in direct_partition_children(catalog, parent.relation_oid)? {
            if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
                continue;
            }
            let existing_bound = child_partition_bound(&child)?;
            if let PartitionBoundSpec::Hash {
                modulus: existing_modulus,
                ..
            } = existing_bound
                && !hash_moduli_compatible(*new_modulus, existing_modulus)
            {
                return Err(ExecError::DetailedError {
                    message:
                        "every hash partition modulus must be a factor of the next larger modulus"
                            .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P17",
                });
            }
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

pub(crate) fn validate_partition_relation_compatibility(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    parent_name: &str,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), ExecError> {
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        return Err(ExecError::Parse(
            crate::backend::parser::ParseError::WrongObjectType {
                name: parent_name.to_string(),
                expected: "partitioned table",
            },
        ));
    }
    if !matches!(child.relkind, 'r' | 'p') {
        return Err(ExecError::Parse(
            crate::backend::parser::ParseError::WrongObjectType {
                name: child_name.to_string(),
                expected: "table",
            },
        ));
    }
    if child.relpersistence != parent.relpersistence {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{child_name}\" would have different persistence than partitioned table \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
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
    if parent_columns.len() != child_columns.len() {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{child_name}\" has different column count than partitioned table \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    for (parent_column, child_column) in parent_columns.iter().zip(child_columns.iter()) {
        if !parent_column.name.eq_ignore_ascii_case(&child_column.name)
            || parent_column.sql_type != child_column.sql_type
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition \"{child_name}\" has different column layout than partitioned table \"{parent_name}\""
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }
    if child.relispartition {
        return Err(ExecError::DetailedError {
            message: format!("table \"{child_name}\" is already a partition"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if !catalog.inheritance_parents(child.relation_oid).is_empty() {
        return Err(ExecError::DetailedError {
            message: format!("table \"{child_name}\" is already a child of another relation"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if child.relkind == 'r' && !catalog.inheritance_children(child.relation_oid).is_empty() {
        return Err(ExecError::DetailedError {
            message: format!("table \"{child_name}\" is already an inheritance parent"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
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
            if candidate_row_matches_partition(catalog, parent, bound, &row, None)? {
                return Err(partition_constraint_violation(
                    &relation_name_for_oid(catalog, relation_oid),
                    &row,
                    &ctx.datetime_config,
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
            if !candidate_row_matches_partition(catalog, parent, bound, &row, None)? {
                return Err(partition_constraint_violation(
                    &relation_name_for_oid(catalog, child.relation_oid),
                    &row,
                    &ctx.datetime_config,
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
    datetime_config: &DateTimeConfig,
) -> Result<BoundRelation, ExecError> {
    let mut proute = exec_setup_partition_tuple_routing(catalog, target)?;
    exec_find_partition(catalog, &mut proute, target, row, datetime_config)
}

pub(crate) fn exec_find_partition(
    catalog: &dyn CatalogLookup,
    proute: &mut PartitionTupleRouting,
    target: &BoundRelation,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<BoundRelation, ExecError> {
    if target.relispartition
        && let Some(parent) = declarative_parent(catalog, target)?
    {
        let selected =
            find_partition_child(catalog, proute, &parent, row)?.map(|lookup| lookup.reldesc);
        if selected
            .as_ref()
            .is_none_or(|relation| relation.relation_oid != target.relation_oid)
        {
            return Err(partition_constraint_violation(
                &relation_name_for_oid(catalog, target.relation_oid),
                row,
                datetime_config,
            ));
        }
    }

    let mut current = target.clone();
    loop {
        if current.relkind != 'p' {
            return Ok(current);
        }

        let Some(selected) = find_partition_child(catalog, proute, &current, row)? else {
            let dispatch = proute.dispatch_info_for_relation(catalog, &current)?;
            return Err(no_partition_for_row(
                &relation_name_for_oid(catalog, current.relation_oid),
                no_partition_detail(&current, &dispatch.key, row, datetime_config)?,
            ));
        };
        if selected.is_leaf {
            return Ok(selected.reldesc);
        }
        current = selected.reldesc;
    }
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
) -> Result<Option<PartitionLookup>, ExecError> {
    let dispatch = proute.dispatch_info_for_relation(catalog, relation)?;
    let Some(part_index) = get_partition_for_tuple(dispatch, row)? else {
        return Ok(None);
    };
    let child = &dispatch.partdesc.children[part_index];
    Ok(Some(PartitionLookup {
        reldesc: child.reldesc.clone(),
        is_leaf: child.is_leaf,
    }))
}
