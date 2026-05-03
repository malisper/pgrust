use std::cmp::Ordering;

use pgrust_catalog_data::pg_proc::{
    BRIN_MINMAX_ADD_VALUE_PROC_OID, BRIN_MINMAX_CONSISTENT_PROC_OID, BRIN_MINMAX_OPCINFO_PROC_OID,
    BRIN_MINMAX_UNION_PROC_OID,
};
use pgrust_nodes::datum::Value;

use crate::access::brin_internal::BrinValues;
use crate::{AccessError, AccessResult, AccessScalarServices};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrinMinmaxStrategy {
    Less = 1,
    LessEqual = 2,
    Equal = 3,
    GreaterEqual = 4,
    Greater = 5,
}

impl TryFrom<i16> for BrinMinmaxStrategy {
    type Error = AccessError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Less),
            2 => Ok(Self::LessEqual),
            3 => Ok(Self::Equal),
            4 => Ok(Self::GreaterEqual),
            5 => Ok(Self::Greater),
            _ => Err(AccessError::Unsupported(format!(
                "unsupported BRIN minmax strategy {value}"
            ))),
        }
    }
}

fn ensure_support_proc(actual: u32, expected: u32, label: &str) -> AccessResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(AccessError::Unsupported(format!(
            "unsupported BRIN minmax {label} proc {actual}"
        )))
    }
}

fn ensure_summary_shape(column: &BrinValues) -> AccessResult<()> {
    if column.values.len() == 2 {
        Ok(())
    } else {
        Err(AccessError::Corrupt(
            "BRIN minmax summary columns must store exactly two values",
        ))
    }
}

pub fn compare_minmax_values(
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<Ordering> {
    match (left, right) {
        (Value::Int16(a), Value::Int16(b)) => Ok(a.cmp(b)),
        (Value::InternalChar(a), Value::InternalChar(b)) => Ok(a.cmp(b)),
        (Value::Null, _) | (_, Value::Null) => Err(AccessError::Corrupt(
            "BRIN minmax comparisons cannot use NULL values",
        )),
        _ => services
            .compare_order_values(left, right, None, Some(false), false)
            .map_err(|err| AccessError::Scalar(format!("BRIN minmax comparison failed: {err}"))),
    }
}

fn interval_contains_value(
    lower: &Value,
    upper: &Value,
    value: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    Ok(
        compare_minmax_values(lower, value, services)? != Ordering::Greater
            && compare_minmax_values(upper, value, services)? != Ordering::Less,
    )
}

fn normalize_interval(
    lower: Value,
    upper: Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<(Value, Value)> {
    if compare_minmax_values(&lower, &upper, services)? == Ordering::Greater {
        Ok((upper, lower))
    } else {
        Ok((lower, upper))
    }
}

fn minmax_multi_intervals(
    column: &BrinValues,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<(Value, Value)>> {
    if column.values.len() < 2 {
        return Err(AccessError::Corrupt(
            "BRIN minmax-multi summary columns must store at least two values",
        ));
    }
    let mut intervals = Vec::new();
    for pair in column.values.chunks(2) {
        let [lower, upper] = pair else {
            continue;
        };
        if matches!(lower, Value::Null) || matches!(upper, Value::Null) {
            continue;
        }
        let interval = normalize_interval(lower.clone(), upper.clone(), services)?;
        if !intervals
            .iter()
            .any(|existing: &(Value, Value)| existing.0 == interval.0 && existing.1 == interval.1)
        {
            intervals.push(interval);
        }
    }
    intervals.sort_by(|left, right| {
        compare_minmax_values(&left.0, &right.0, services)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                compare_minmax_values(&left.1, &right.1, services).unwrap_or(Ordering::Equal)
            })
    });
    Ok(intervals)
}

fn merge_interval_at(
    intervals: &mut Vec<(Value, Value)>,
    index: usize,
    services: &dyn AccessScalarServices,
) -> AccessResult<()> {
    let right = intervals.remove(index + 1);
    let left = &mut intervals[index];
    if compare_minmax_values(&right.1, &left.1, services)? == Ordering::Greater {
        left.1 = right.1;
    }
    Ok(())
}

fn minmax_multi_distance_key(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(value) => Some(i128::from(*value)),
        Value::Int32(value) => Some(i128::from(*value)),
        Value::EnumOid(value) => Some(i128::from(*value)),
        Value::Int64(value) | Value::Money(value) => Some(i128::from(*value)),
        Value::Date(value) => Some(i128::from(value.0)),
        Value::Time(value) => Some(i128::from(value.0)),
        Value::Timestamp(value) => Some(i128::from(value.0)),
        Value::TimestampTz(value) => Some(i128::from(value.0)),
        Value::Interval(value) => Some(value.cmp_key()),
        Value::PgLsn(value) => Some(i128::from(*value)),
        Value::Tid(value) => Some(
            i128::from(value.block_number) * i128::from(u16::MAX) + i128::from(value.offset_number),
        ),
        Value::Uuid(bytes) => Some(u128::from_be_bytes(*bytes).min(i128::MAX as u128) as i128),
        _ => None,
    }
}

fn minmax_multi_gap(left: &(Value, Value), right: &(Value, Value)) -> Option<i128> {
    let upper = minmax_multi_distance_key(&left.1)?;
    let lower = minmax_multi_distance_key(&right.0)?;
    Some(lower.saturating_sub(upper).max(0))
}

fn closest_interval_pair(intervals: &[(Value, Value)]) -> usize {
    intervals
        .windows(2)
        .enumerate()
        .filter_map(|(index, pair)| minmax_multi_gap(&pair[0], &pair[1]).map(|gap| (index, gap)))
        .min_by_key(|(_, gap)| *gap)
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn compact_intervals(
    intervals: &mut Vec<(Value, Value)>,
    capacity: usize,
    services: &dyn AccessScalarServices,
) -> AccessResult<()> {
    intervals.sort_by(|left, right| {
        compare_minmax_values(&left.0, &right.0, services)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                compare_minmax_values(&left.1, &right.1, services).unwrap_or(Ordering::Equal)
            })
    });
    let mut index = 0;
    while index + 1 < intervals.len() {
        if compare_minmax_values(&intervals[index + 1].0, &intervals[index].1, services)?
            != Ordering::Greater
        {
            merge_interval_at(intervals, index, services)?;
        } else {
            index += 1;
        }
    }
    while intervals.len() > capacity {
        let index = closest_interval_pair(intervals);
        merge_interval_at(intervals, index, services)?;
    }
    Ok(())
}

fn store_minmax_multi_intervals(column: &mut BrinValues, intervals: &[(Value, Value)]) {
    if intervals.is_empty() {
        column.all_nulls = true;
        column.values.fill(Value::Null);
        return;
    }
    let mut stored = Vec::with_capacity(column.values.len());
    for (lower, upper) in intervals {
        if stored.len() + 1 >= column.values.len() {
            break;
        }
        stored.push(lower.clone());
        stored.push(upper.clone());
    }
    while stored.len() < column.values.len() {
        let (lower, upper) = intervals.last().expect("intervals is not empty");
        stored.push(lower.clone());
        if stored.len() < column.values.len() {
            stored.push(upper.clone());
        }
    }
    column.values = stored;
    column.all_nulls = false;
}

pub fn minmax_multi_add_value(
    column: &mut BrinValues,
    new_value: &Value,
    is_null: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if is_null || matches!(new_value, Value::Null) {
        return Err(AccessError::Corrupt(
            "BRIN minmax-multi add_value received NULL input",
        ));
    }
    let capacity = (column.values.len() / 2).max(1);
    if column.all_nulls {
        store_minmax_multi_intervals(column, &[(new_value.clone(), new_value.clone())]);
        return Ok(true);
    }

    let mut intervals = minmax_multi_intervals(column, services)?;
    for (lower, upper) in &intervals {
        if interval_contains_value(lower, upper, new_value, services)? {
            return Ok(false);
        }
    }
    intervals.push((new_value.clone(), new_value.clone()));
    compact_intervals(&mut intervals, capacity, services)?;
    store_minmax_multi_intervals(column, &intervals);
    Ok(true)
}

pub fn minmax_multi_consistent(
    column: &BrinValues,
    strategy: BrinMinmaxStrategy,
    scan_value: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if column.all_nulls {
        return Ok(false);
    }
    for (lower, upper) in minmax_multi_intervals(column, services)? {
        let matches = match strategy {
            BrinMinmaxStrategy::Less => {
                compare_minmax_values(&lower, scan_value, services)? == Ordering::Less
            }
            BrinMinmaxStrategy::LessEqual => {
                compare_minmax_values(&lower, scan_value, services)? != Ordering::Greater
            }
            BrinMinmaxStrategy::Equal => {
                interval_contains_value(&lower, &upper, scan_value, services)?
            }
            BrinMinmaxStrategy::GreaterEqual => {
                compare_minmax_values(&upper, scan_value, services)? != Ordering::Less
            }
            BrinMinmaxStrategy::Greater => {
                compare_minmax_values(&upper, scan_value, services)? == Ordering::Greater
            }
        };
        if matches {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn minmax_multi_union(
    left: &mut BrinValues,
    right: &BrinValues,
    services: &dyn AccessScalarServices,
) -> AccessResult<()> {
    left.has_nulls |= right.has_nulls;
    if right.all_nulls {
        return Ok(());
    }
    if left.all_nulls {
        left.values = right.values.clone();
        left.all_nulls = false;
        return Ok(());
    }
    let capacity = (left.values.len() / 2).max(1);
    let mut intervals = minmax_multi_intervals(left, services)?;
    intervals.extend(minmax_multi_intervals(right, services)?);
    compact_intervals(&mut intervals, capacity, services)?;
    store_minmax_multi_intervals(left, &intervals);
    Ok(())
}

pub fn minmax_opcinfo(proc_oid: u32) -> AccessResult<(usize, bool)> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_OPCINFO_PROC_OID, "opcinfo")?;
    Ok((2, true))
}

pub fn minmax_add_value(
    proc_oid: u32,
    column: &mut BrinValues,
    new_value: &Value,
    is_null: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_ADD_VALUE_PROC_OID, "add_value")?;
    ensure_summary_shape(column)?;
    if is_null || matches!(new_value, Value::Null) {
        return Err(AccessError::Corrupt(
            "BRIN minmax add_value received NULL input",
        ));
    }

    if column.all_nulls {
        column.values[0] = new_value.clone();
        column.values[1] = new_value.clone();
        column.all_nulls = false;
        return Ok(true);
    }

    let mut updated = false;
    if compare_minmax_values(new_value, &column.values[0], services)? == Ordering::Less {
        column.values[0] = new_value.clone();
        updated = true;
    }
    if compare_minmax_values(new_value, &column.values[1], services)? == Ordering::Greater {
        column.values[1] = new_value.clone();
        updated = true;
    }
    Ok(updated)
}

pub fn minmax_consistent(
    proc_oid: u32,
    column: &BrinValues,
    strategy: BrinMinmaxStrategy,
    scan_value: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_CONSISTENT_PROC_OID, "consistent")?;
    ensure_summary_shape(column)?;
    if column.all_nulls {
        return Ok(false);
    }

    match strategy {
        BrinMinmaxStrategy::Less => {
            Ok(compare_minmax_values(&column.values[0], scan_value, services)? == Ordering::Less)
        }
        BrinMinmaxStrategy::LessEqual => Ok(compare_minmax_values(
            &column.values[0],
            scan_value,
            services,
        )? != Ordering::Greater),
        BrinMinmaxStrategy::Equal => {
            let min_matches = compare_minmax_values(&column.values[0], scan_value, services)?
                != Ordering::Greater;
            let max_matches =
                compare_minmax_values(&column.values[1], scan_value, services)? != Ordering::Less;
            Ok(min_matches && max_matches)
        }
        BrinMinmaxStrategy::GreaterEqual => {
            Ok(compare_minmax_values(&column.values[1], scan_value, services)? != Ordering::Less)
        }
        BrinMinmaxStrategy::Greater => Ok(compare_minmax_values(
            &column.values[1],
            scan_value,
            services,
        )? == Ordering::Greater),
    }
}

pub fn minmax_union(
    proc_oid: u32,
    left: &mut BrinValues,
    right: &BrinValues,
    services: &dyn AccessScalarServices,
) -> AccessResult<()> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_UNION_PROC_OID, "union")?;
    ensure_summary_shape(left)?;
    ensure_summary_shape(right)?;

    left.has_nulls |= right.has_nulls;
    if right.all_nulls {
        return Ok(());
    }
    if left.all_nulls {
        left.values = right.values.clone();
        left.all_nulls = false;
        return Ok(());
    }

    if compare_minmax_values(&right.values[0], &left.values[0], services)? == Ordering::Less {
        left.values[0] = right.values[0].clone();
    }
    if compare_minmax_values(&right.values[1], &left.values[1], services)? == Ordering::Greater {
        left.values[1] = right.values[1].clone();
    }
    Ok(())
}
