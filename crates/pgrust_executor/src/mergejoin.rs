use std::cmp::Ordering;

use pgrust_nodes::Value;

pub type MergeKey = Vec<Value>;

#[derive(Debug)]
pub struct MergeJoinBufferedRow<Row> {
    pub row: Row,
    pub key: MergeKey,
    pub matchable: bool,
    pub matched: bool,
}

pub fn compare_merge_keys<E>(
    collations: &[Option<u32>],
    descending: &[bool],
    left: &[Value],
    right: &[Value],
    mut compare_value: impl FnMut(&Value, &Value, Option<u32>) -> Result<Ordering, E>,
) -> Result<Ordering, E> {
    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        let descending = descending.get(index).copied().unwrap_or(false);
        let collation = collations.get(index).copied().flatten();
        let mut ordering = compare_value(left_value, right_value, collation)?;
        if descending {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

pub fn same_merge_key<E>(
    collations: &[Option<u32>],
    left: &MergeKey,
    right: &MergeKey,
    compare_value: impl FnMut(&Value, &Value, Option<u32>) -> Result<Ordering, E>,
) -> Result<bool, E> {
    Ok(compare_merge_keys(collations, &[], left, right, compare_value)? == Ordering::Equal)
}

pub fn group_end_by_merge_key<Row, E>(
    rows: &[MergeJoinBufferedRow<Row>],
    start: usize,
    collations: &[Option<u32>],
    mut compare_value: impl FnMut(&Value, &Value, Option<u32>) -> Result<Ordering, E>,
) -> Result<usize, E> {
    let first_key = &rows[start].key;
    let mut end = start + 1;
    while end < rows.len() {
        let next_key = &rows[end].key;
        if !same_merge_key(collations, first_key, next_key, &mut compare_value)? {
            break;
        }
        end += 1;
    }
    Ok(end)
}

pub fn combined_join_values(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut values = left.to_vec();
    values.extend(right.iter().cloned());
    values
}

pub fn null_extended_left_values(left: &[Value], right_width: usize) -> Vec<Value> {
    let mut values = left.to_vec();
    values.extend(std::iter::repeat_n(Value::Null, right_width));
    values
}

pub fn null_extended_right_values(right: &[Value], left_width: usize) -> Vec<Value> {
    let mut values = vec![Value::Null; left_width];
    values.extend(right.iter().cloned());
    values
}
