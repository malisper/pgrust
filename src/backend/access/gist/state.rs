use crate::backend::catalog::CatalogError;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::gist::{
    GIST_CONSISTENT_PROC, GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_PENALTY_PROC,
    GIST_PICKSPLIT_PROC, GIST_SORTSUPPORT_PROC, GIST_TRANSLATE_CMPTYPE_PROC, GIST_UNION_PROC,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

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

#[derive(Debug, Clone)]
pub(crate) struct GistState {
    pub(crate) columns: Vec<GistColumnState>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GistPageSplit {
    pub(crate) left: Vec<usize>,
    pub(crate) right: Vec<usize>,
    pub(crate) left_union: Vec<Value>,
    pub(crate) right_union: Vec<Value>,
}

impl GistState {
    pub(crate) fn new(
        desc: &RelationDesc,
        index_meta: &IndexRelCacheEntry,
    ) -> Result<Self, CatalogError> {
        let key_count = usize::try_from(index_meta.indnkeyatts.max(0)).unwrap_or_default();
        let mut columns = Vec::with_capacity(key_count);
        for column_index in 0..key_count.min(desc.columns.len()) {
            columns.push(GistColumnState {
                consistent_proc: index_meta
                    .amproc_oid(desc, column_index, GIST_CONSISTENT_PROC)
                    .ok_or(CatalogError::Corrupt(
                        "missing GiST consistent support proc",
                    ))?,
                union_proc: index_meta
                    .amproc_oid(desc, column_index, GIST_UNION_PROC)
                    .ok_or(CatalogError::Corrupt("missing GiST union support proc"))?,
                penalty_proc: index_meta
                    .amproc_oid(desc, column_index, GIST_PENALTY_PROC)
                    .ok_or(CatalogError::Corrupt("missing GiST penalty support proc"))?,
                picksplit_proc: index_meta
                    .amproc_oid(desc, column_index, GIST_PICKSPLIT_PROC)
                    .ok_or(CatalogError::Corrupt("missing GiST picksplit support proc"))?,
                same_proc: index_meta
                    .amproc_oid(desc, column_index, GIST_EQUAL_PROC)
                    .ok_or(CatalogError::Corrupt("missing GiST same support proc"))?,
                distance_proc: index_meta.amproc_oid(desc, column_index, GIST_DISTANCE_PROC),
                sortsupport_proc: index_meta.amproc_oid(desc, column_index, GIST_SORTSUPPORT_PROC),
                translate_cmptype_proc: index_meta.amproc_oid(
                    desc,
                    column_index,
                    GIST_TRANSLATE_CMPTYPE_PROC,
                ),
            });
        }
        Ok(Self { columns })
    }

    pub(crate) fn union_all(&self, items: &[Vec<Value>]) -> Result<Vec<Value>, CatalogError> {
        let mut unions = Vec::with_capacity(self.columns.len());
        for column_index in 0..self.columns.len() {
            let column_values = items
                .iter()
                .filter_map(|values| values.get(column_index).cloned())
                .collect::<Vec<_>>();
            unions.push(union(
                self.columns[column_index].union_proc,
                &column_values,
            )?);
        }
        Ok(unions)
    }

    pub(crate) fn merge_values(
        &self,
        left: &[Value],
        right: &[Value],
    ) -> Result<Vec<Value>, CatalogError> {
        let mut merged = Vec::with_capacity(self.columns.len());
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let values = [
                left.get(column_index).cloned().unwrap_or(Value::Null),
                right.get(column_index).cloned().unwrap_or(Value::Null),
            ];
            merged.push(union(column_state.union_proc, &values)?);
        }
        Ok(merged)
    }

    pub(crate) fn same_values(
        &self,
        left: &[Value],
        right: &[Value],
    ) -> Result<bool, CatalogError> {
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let left_value = left.get(column_index).cloned().unwrap_or(Value::Null);
            let right_value = right.get(column_index).cloned().unwrap_or(Value::Null);
            if !same(column_state.same_proc, &left_value, &right_value)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn aggregate_penalty(
        &self,
        original: &[Value],
        candidate: &[Value],
    ) -> Result<f32, CatalogError> {
        Ok(self
            .column_penalties(original, candidate)?
            .into_iter()
            .sum::<f32>())
    }

    pub(crate) fn column_penalties(
        &self,
        original: &[Value],
        candidate: &[Value],
    ) -> Result<Vec<f32>, CatalogError> {
        let mut total = 0.0f32;
        let mut penalties = Vec::with_capacity(self.columns.len());
        for (column_index, column_state) in self.columns.iter().enumerate() {
            let original_value = original.get(column_index).cloned().unwrap_or(Value::Null);
            let candidate_value = candidate.get(column_index).cloned().unwrap_or(Value::Null);
            let column_penalty =
                penalty(column_state.penalty_proc, &original_value, &candidate_value)?;
            total += column_penalty;
            penalties.push(column_penalty);
        }
        debug_assert!((penalties.iter().sum::<f32>() - total).abs() < f32::EPSILON * 8.0);
        Ok(penalties)
    }

    pub(crate) fn picksplit(&self, items: &[Vec<Value>]) -> Result<GistPageSplit, CatalogError> {
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
    ) -> Result<(Vec<usize>, Vec<usize>), CatalogError> {
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
        let column_split = picksplit(self.columns[column_index].picksplit_proc, &driver_values)?;
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
    ) -> Result<GistConsistentResult, CatalogError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(CatalogError::Corrupt("gist scan key attno out of range"))?;
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
                .ok_or(CatalogError::Corrupt("gist column state missing"))?
                .consistent_proc,
            key.strategy,
            tuple_value,
            &key.argument,
            is_leaf,
        )
    }

    pub(crate) fn distance(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
        is_leaf: bool,
    ) -> Result<GistDistanceResult, CatalogError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(CatalogError::Corrupt("gist order-by attno out of range"))?;
        let column_state = self
            .columns
            .get(attno)
            .ok_or(CatalogError::Corrupt("gist column state missing"))?;
        let proc_oid = column_state.distance_proc.ok_or(CatalogError::Io(
            "GiST ORDER BY requires distance support proc".into(),
        ))?;
        if let Some(proc_oid) = column_state.translate_cmptype_proc {
            let _ = translate_cmptype(proc_oid, std::cmp::Ordering::Equal)?;
        }
        if let Some(proc_oid) = column_state.sortsupport_proc {
            let _ = sortsupport(proc_oid);
        }
        distance(proc_oid, tuple_value, &key.argument, is_leaf)
    }
}

#[cfg(test)]
mod tests {
    use super::{GistColumnState, GistState};
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::catalog::{
        GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_PENALTY_PROC_OID,
        GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID,
    };
    use crate::include::nodes::datum::{GeoBox, GeoPoint, Value};

    fn box_value(low_x: f64, low_y: f64, high_x: f64, high_y: f64) -> Value {
        Value::Box(GeoBox {
            low: GeoPoint { x: low_x, y: low_y },
            high: GeoPoint {
                x: high_x,
                y: high_y,
            },
        })
    }

    fn box_state() -> GistColumnState {
        GistColumnState {
            consistent_proc: GIST_BOX_CONSISTENT_PROC_OID,
            union_proc: GIST_BOX_UNION_PROC_OID,
            penalty_proc: GIST_BOX_PENALTY_PROC_OID,
            picksplit_proc: GIST_BOX_PICKSPLIT_PROC_OID,
            same_proc: GIST_BOX_SAME_PROC_OID,
            distance_proc: Some(GIST_BOX_DISTANCE_PROC_OID),
            sortsupport_proc: None,
            translate_cmptype_proc: None,
        }
    }

    #[test]
    fn picksplit_recurses_to_later_columns_when_leading_column_is_null() {
        let state = GistState {
            columns: vec![box_state(), box_state()],
        };
        let items = vec![
            vec![Value::Null, box_value(0.0, 0.0, 1.0, 1.0)],
            vec![Value::Null, box_value(1.0, 1.0, 2.0, 2.0)],
            vec![Value::Null, box_value(10.0, 10.0, 11.0, 11.0)],
            vec![Value::Null, box_value(11.0, 11.0, 12.0, 12.0)],
        ];

        let split = state.picksplit(&items).unwrap();

        assert!(!split.left.is_empty());
        assert!(!split.right.is_empty());
        assert_eq!(split.left_union[0], Value::Null);
        assert_eq!(split.right_union[0], Value::Null);
        let mut all_indexes = split.left.clone();
        all_indexes.extend(split.right.iter().copied());
        all_indexes.sort_unstable();
        assert_eq!(all_indexes, vec![0, 1, 2, 3]);
    }

    #[test]
    fn picksplit_routes_null_keys_to_right_partition() {
        let state = GistState {
            columns: vec![box_state(), box_state()],
        };
        let items = vec![
            vec![box_value(0.0, 0.0, 1.0, 1.0), box_value(0.0, 0.0, 1.0, 1.0)],
            vec![box_value(1.0, 1.0, 2.0, 2.0), box_value(1.0, 1.0, 2.0, 2.0)],
            vec![Value::Null, box_value(50.0, 50.0, 51.0, 51.0)],
            vec![
                box_value(10.0, 10.0, 11.0, 11.0),
                box_value(10.0, 10.0, 11.0, 11.0),
            ],
        ];

        let split = state.picksplit(&items).unwrap();

        assert!(split.right.contains(&2));
    }

    #[test]
    fn column_penalties_preserve_per_column_penalty_ordering() {
        let state = GistState {
            columns: vec![box_state(), box_state()],
        };
        let original = vec![box_value(0.0, 0.0, 1.0, 1.0), box_value(0.0, 0.0, 1.0, 1.0)];
        let candidate = vec![
            box_value(0.0, 0.0, 2.0, 2.0),
            box_value(10.0, 10.0, 11.0, 11.0),
        ];

        let penalties = state.column_penalties(&original, &candidate).unwrap();

        assert_eq!(penalties.len(), 2);
        assert!(penalties[1] > penalties[0]);
        assert!(state.aggregate_penalty(&original, &candidate).unwrap() > 0.0);
    }

    #[test]
    fn box_relation_desc_is_constructible_for_gist_tests() {
        let desc = crate::include::nodes::primnodes::RelationDesc {
            columns: vec![
                column_desc("a", SqlType::new(SqlTypeKind::Box), true),
                column_desc("b", SqlType::new(SqlTypeKind::Box), true),
            ],
        };
        assert_eq!(desc.columns.len(), 2);
    }
}
