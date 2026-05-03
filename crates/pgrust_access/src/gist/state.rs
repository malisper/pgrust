use pgrust_catalog_data::{
    ANYELEMENTOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID, BIT_TYPE_OID, CIDR_TYPE_OID,
    INET_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, VARBIT_TYPE_OID,
    builtin_range_spec_by_multirange_oid, builtin_range_spec_by_oid, sql_type_oid,
};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::relcache::IndexRelCacheEntry;

use crate::access::gist::{
    GIST_CONSISTENT_PROC, GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_PENALTY_PROC,
    GIST_PICKSPLIT_PROC, GIST_SORTSUPPORT_PROC, GIST_TRANSLATE_CMPTYPE_PROC, GIST_UNION_PROC,
};
use crate::access::scankey::ScanKeyData;
use crate::{AccessError, AccessScalarServices};

use super::support::{
    GistConsistentResult, GistDistanceResult, consistent, distance, penalty, picksplit, same,
    sortsupport, translate_cmptype, union,
};

#[derive(Debug, Clone)]
pub(crate) struct GistColumnState {
    pub(crate) consistent_proc: u32,
    pub(crate) union_proc: u32,
    pub(crate) penalty_proc: u32,
    pub(crate) picksplit_proc: u32,
    pub(crate) same_proc: u32,
    pub(crate) distance_proc: Option<u32>,
    pub(crate) sortsupport_proc: Option<u32>,
    pub(crate) translate_cmptype_proc: Option<u32>,
}

#[derive(Clone)]
pub(crate) struct GistState<'a> {
    pub(crate) columns: Vec<GistColumnState>,
    scalar: &'a dyn AccessScalarServices,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GistPageSplit {
    pub(crate) left: Vec<usize>,
    pub(crate) right: Vec<usize>,
    pub(crate) left_union: Vec<Value>,
    pub(crate) right_union: Vec<Value>,
}

impl<'a> GistState<'a> {
    pub(crate) fn scalar_services(&self) -> &'a dyn AccessScalarServices {
        self.scalar
    }

    pub(crate) fn new(
        desc: &RelationDesc,
        index_meta: &IndexRelCacheEntry,
        scalar: &'a dyn AccessScalarServices,
    ) -> Result<Self, AccessError> {
        let key_count = usize::try_from(index_meta.indnkeyatts.max(0)).unwrap_or_default();
        let mut columns = Vec::with_capacity(key_count);
        for column_index in 0..key_count.min(desc.columns.len()) {
            columns.push(GistColumnState {
                consistent_proc: index_amproc_oid(
                    index_meta,
                    desc,
                    column_index,
                    GIST_CONSISTENT_PROC,
                )
                .ok_or(AccessError::Corrupt("missing GiST consistent support proc"))?,
                union_proc: index_amproc_oid(index_meta, desc, column_index, GIST_UNION_PROC)
                    .ok_or(AccessError::Corrupt("missing GiST union support proc"))?,
                penalty_proc: index_amproc_oid(index_meta, desc, column_index, GIST_PENALTY_PROC)
                    .ok_or(AccessError::Corrupt("missing GiST penalty support proc"))?,
                picksplit_proc: index_amproc_oid(
                    index_meta,
                    desc,
                    column_index,
                    GIST_PICKSPLIT_PROC,
                )
                .ok_or(AccessError::Corrupt("missing GiST picksplit support proc"))?,
                same_proc: index_amproc_oid(index_meta, desc, column_index, GIST_EQUAL_PROC)
                    .ok_or(AccessError::Corrupt("missing GiST same support proc"))?,
                distance_proc: index_amproc_oid(index_meta, desc, column_index, GIST_DISTANCE_PROC),
                sortsupport_proc: index_amproc_oid(
                    index_meta,
                    desc,
                    column_index,
                    GIST_SORTSUPPORT_PROC,
                ),
                translate_cmptype_proc: index_amproc_oid(
                    index_meta,
                    desc,
                    column_index,
                    GIST_TRANSLATE_CMPTYPE_PROC,
                ),
            });
        }
        Ok(Self { columns, scalar })
    }

    pub(crate) fn union_all(&self, items: &[Vec<Value>]) -> Result<Vec<Value>, AccessError> {
        let mut unions = Vec::with_capacity(self.columns.len());
        for column_index in 0..self.columns.len() {
            let column_values = items
                .iter()
                .filter_map(|values| values.get(column_index).cloned())
                .collect::<Vec<_>>();
            unions.push(union(
                self.columns[column_index].union_proc,
                &column_values,
                self.scalar,
            )?);
        }
        Ok(unions)
    }

    pub(crate) fn merge_values(
        &self,
        left: &[Value],
        right: &[Value],
    ) -> Result<Vec<Value>, AccessError> {
        let mut merged = Vec::with_capacity(self.columns.len());
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let values = [
                left.get(column_index).cloned().unwrap_or(Value::Null),
                right.get(column_index).cloned().unwrap_or(Value::Null),
            ];
            merged.push(union(column_state.union_proc, &values, self.scalar)?);
        }
        Ok(merged)
    }

    pub(crate) fn same_values(&self, left: &[Value], right: &[Value]) -> Result<bool, AccessError> {
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let left_value = left.get(column_index).cloned().unwrap_or(Value::Null);
            let right_value = right.get(column_index).cloned().unwrap_or(Value::Null);
            if !same(
                column_state.same_proc,
                &left_value,
                &right_value,
                self.scalar,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn column_penalties(
        &self,
        original: &[Value],
        candidate: &[Value],
    ) -> Result<Vec<f32>, AccessError> {
        let mut total = 0.0f32;
        let mut penalties = Vec::with_capacity(self.columns.len());
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let original_value = original.get(column_index).cloned().unwrap_or(Value::Null);
            let candidate_value = candidate.get(column_index).cloned().unwrap_or(Value::Null);
            let column_penalty = penalty(
                column_state.penalty_proc,
                &original_value,
                &candidate_value,
                self.scalar,
            )?;
            total += column_penalty;
            penalties.push(column_penalty);
        }
        debug_assert!((penalties.iter().sum::<f32>() - total).abs() < f32::EPSILON * 8.0);
        Ok(penalties)
    }

    pub(crate) fn picksplit(&self, items: &[Vec<Value>]) -> Result<GistPageSplit, AccessError> {
        if items.len() <= 1 {
            let union = self.union_all(items)?;
            return Ok(GistPageSplit {
                left: vec![0],
                right: Vec::new(),
                left_union: union.clone(),
                right_union: union,
            });
        }

        let indexes = (0..items.len()).collect::<Vec<_>>();
        let (left, right) = self.picksplit_by_column(items, &indexes, 0)?;
        Ok(self.materialize_split(items, left, right))
    }

    fn materialize_split(
        &self,
        items: &[Vec<Value>],
        left: Vec<usize>,
        right: Vec<usize>,
    ) -> GistPageSplit {
        let left_union = self
            .union_all(
                &left
                    .iter()
                    .map(|index| items[*index].clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| vec![Value::Null; self.columns.len()]);
        let right_union = self
            .union_all(
                &right
                    .iter()
                    .map(|index| items[*index].clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| vec![Value::Null; self.columns.len()]);
        GistPageSplit {
            left,
            right,
            left_union,
            right_union,
        }
    }

    fn picksplit_by_column(
        &self,
        items: &[Vec<Value>],
        indexes: &[usize],
        column_index: usize,
    ) -> Result<(Vec<usize>, Vec<usize>), AccessError> {
        if indexes.len() <= 1 {
            return Ok((indexes.to_vec(), Vec::new()));
        }
        if column_index >= self.columns.len() {
            return Ok(self.fallback_split(indexes));
        }

        let mut non_null_indexes = Vec::new();
        let mut null_indexes = Vec::new();
        for &item_index in indexes {
            if matches!(
                items[item_index]
                    .get(column_index)
                    .cloned()
                    .unwrap_or(Value::Null),
                Value::Null
            ) {
                null_indexes.push(item_index);
            } else {
                non_null_indexes.push(item_index);
            }
        }

        if non_null_indexes.len() <= 1 {
            if column_index + 1 < self.columns.len() {
                return self.picksplit_by_column(items, indexes, column_index + 1);
            }
            return Ok(self.fallback_split(indexes));
        }

        let driver_values = non_null_indexes
            .iter()
            .map(|index| {
                items[*index]
                    .get(column_index)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect::<Vec<_>>();
        let column_split = picksplit(
            self.columns[column_index].picksplit_proc,
            &driver_values,
            self.scalar,
        )?;
        let left = column_split
            .left
            .into_iter()
            .filter_map(|relative| non_null_indexes.get(relative).copied())
            .collect::<Vec<_>>();
        let mut right = column_split
            .right
            .into_iter()
            .filter_map(|relative| non_null_indexes.get(relative).copied())
            .collect::<Vec<_>>();

        if left.is_empty() || right.is_empty() {
            if column_index + 1 < self.columns.len() {
                return self.picksplit_by_column(items, indexes, column_index + 1);
            }
            return Ok(self.fallback_split(indexes));
        }

        right.extend(null_indexes);
        if left.is_empty() || right.is_empty() {
            return Ok(self.fallback_split(indexes));
        }
        Ok((left, right))
    }

    fn fallback_split(&self, indexes: &[usize]) -> (Vec<usize>, Vec<usize>) {
        let split_at = (indexes.len() / 2).max(1);
        (indexes[..split_at].to_vec(), indexes[split_at..].to_vec())
    }

    pub(crate) fn consistent(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
        is_leaf: bool,
    ) -> Result<GistConsistentResult, AccessError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(AccessError::Corrupt("gist scan key attno out of range"))?;
        if matches!(key.argument, Value::Null) {
            return match key.strategy {
                0 => Ok(GistConsistentResult {
                    matches: !is_leaf || matches!(tuple_value, Value::Null),
                    recheck: false,
                }),
                1 => Ok(GistConsistentResult {
                    matches: !is_leaf || !matches!(tuple_value, Value::Null),
                    recheck: false,
                }),
                _ => Ok(GistConsistentResult {
                    matches: false,
                    recheck: false,
                }),
            };
        }
        consistent(
            self.columns
                .get(attno)
                .ok_or(AccessError::Corrupt("gist column state missing"))?
                .consistent_proc,
            key.strategy,
            tuple_value,
            &key.argument,
            is_leaf,
            self.scalar,
        )
    }

    pub(crate) fn distance(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
        is_leaf: bool,
    ) -> Result<GistDistanceResult, AccessError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(AccessError::Corrupt("gist order-by attno out of range"))?;
        let column_state = self
            .columns
            .get(attno)
            .ok_or(AccessError::Corrupt("gist column state missing"))?;
        let proc_oid = column_state.distance_proc.ok_or(AccessError::Scalar(
            "GiST ORDER BY requires distance support proc".into(),
        ))?;
        if let Some(proc_oid) = column_state.translate_cmptype_proc {
            let _ = translate_cmptype(proc_oid, std::cmp::Ordering::Equal)?;
        }
        if let Some(proc_oid) = column_state.sortsupport_proc {
            let _ = sortsupport(proc_oid);
        }
        distance(proc_oid, tuple_value, &key.argument, is_leaf, self.scalar)
    }
}

fn index_indexed_operator_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opcintype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .filter(|oid| !matches!(*oid, ANYOID | ANYRANGEOID | ANYMULTIRANGEOID))
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_indexed_operand_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opckeytype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_type_match_score(
    entry_lefttype: u32,
    entry_righttype: u32,
    left_type_oid: Option<u32>,
    right_type_oid: Option<u32>,
) -> Option<u8> {
    fn same_index_type_family(entry_type: u32, actual_type: u32) -> bool {
        matches!(
            (entry_type, actual_type),
            (INET_TYPE_OID | CIDR_TYPE_OID, INET_TYPE_OID | CIDR_TYPE_OID)
                | (
                    BIT_TYPE_OID | VARBIT_TYPE_OID,
                    BIT_TYPE_OID | VARBIT_TYPE_OID
                )
                | (
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID,
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
                )
        )
    }

    fn component_score(entry_type: u32, actual_type: Option<u32>) -> Option<u8> {
        match actual_type {
            None => Some(0),
            Some(actual) if entry_type == actual => Some(4),
            Some(actual) if same_index_type_family(entry_type, actual) => Some(3),
            Some(_) if entry_type == ANYOID => Some(1),
            Some(actual)
                if entry_type == ANYRANGEOID && builtin_range_spec_by_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(actual)
                if entry_type == ANYMULTIRANGEOID
                    && builtin_range_spec_by_multirange_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(_) if entry_type == ANYELEMENTOID => Some(1),
            Some(_) => None,
        }
    }

    Some(
        component_score(entry_lefttype, left_type_oid)?
            + component_score(entry_righttype, right_type_oid)?,
    )
}

fn index_amproc_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    let operand_type_oid = index_indexed_operand_type_oid(index, desc, column_index);
    let operator_type_oid = index_indexed_operator_type_oid(index, desc, column_index);
    let mut best: Option<(u8, u32)> = None;
    for entry in index.amproc_entries.get(column_index)?.iter() {
        if entry.procnum != procnum {
            continue;
        }
        let operand_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operand_type_oid,
            operand_type_oid,
        );
        let operator_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operator_type_oid,
            operator_type_oid,
        );
        let Some(score) = operand_score.or(operator_score) else {
            continue;
        };
        if best.is_none_or(|(best_score, _)| score > best_score) {
            best = Some((score, entry.proc_oid));
        }
    }
    best.map(|(_, proc_oid)| proc_oid)
}
