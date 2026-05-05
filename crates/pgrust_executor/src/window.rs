use std::cmp::Ordering;

use pgrust_expr::utils::time::datetime::{
    days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use pgrust_nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, USECS_PER_DAY,
};
use pgrust_nodes::datum::{IntervalValue, NumericValue, Value};
use pgrust_nodes::parsenodes::WindowFrameExclusion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFrameError {
    UnsupportedRangeOffset,
    InvalidRangeOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowRangeOrder {
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowRangeRow {
    pub order_keys: Vec<Value>,
}

pub fn rows_frame_start(len: usize, row_index: usize, offset: i64, following: bool) -> usize {
    if following {
        row_index.saturating_add(offset as usize).min(len)
    } else {
        row_index.saturating_sub(offset as usize)
    }
}

pub fn rows_frame_end(len: usize, row_index: usize, offset: i64, following: bool) -> usize {
    if following {
        row_index
            .saturating_add(offset as usize)
            .saturating_add(1)
            .min(len)
    } else {
        row_index
            .checked_sub(offset as usize)
            .map(|value| value + 1)
            .unwrap_or(0)
    }
}

pub fn row_is_included_by_frame_exclusion<E>(
    exclusion: WindowFrameExclusion,
    row_index: usize,
    candidate_index: usize,
    mut same_peer: impl FnMut(usize, usize) -> Result<bool, E>,
) -> Result<bool, E> {
    match exclusion {
        WindowFrameExclusion::NoOthers => Ok(true),
        WindowFrameExclusion::CurrentRow => Ok(candidate_index != row_index),
        WindowFrameExclusion::Group => Ok(!same_peer(row_index, candidate_index)?),
        WindowFrameExclusion::Ties => {
            if candidate_index == row_index {
                return Ok(true);
            }
            Ok(!same_peer(row_index, candidate_index)?)
        }
    }
}

pub fn first_included_frame_row_index<E>(
    exclusion: WindowFrameExclusion,
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
    mut same_peer: impl FnMut(usize, usize) -> Result<bool, E>,
) -> Result<Option<usize>, E> {
    for candidate in frame_start..frame_end {
        if row_is_included_by_frame_exclusion(exclusion, row_index, candidate, &mut same_peer)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

pub fn last_included_frame_row_index<E>(
    exclusion: WindowFrameExclusion,
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
    mut same_peer: impl FnMut(usize, usize) -> Result<bool, E>,
) -> Result<Option<usize>, E> {
    for candidate in (frame_start..frame_end).rev() {
        if row_is_included_by_frame_exclusion(exclusion, row_index, candidate, &mut same_peer)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

pub fn nth_included_frame_row_index<E>(
    exclusion: WindowFrameExclusion,
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
    nth: usize,
    mut same_peer: impl FnMut(usize, usize) -> Result<bool, E>,
) -> Result<Option<usize>, E> {
    let mut seen = 0usize;
    for candidate in frame_start..frame_end {
        if row_is_included_by_frame_exclusion(exclusion, row_index, candidate, &mut same_peer)? {
            seen += 1;
            if seen == nth {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

pub fn range_frame_start_from_offset(
    partition_rows: &[WindowRangeRow],
    order: WindowRangeOrder,
    row_index: usize,
    offset: &Value,
    following: bool,
) -> Result<usize, WindowFrameError> {
    let current_key = &partition_rows[row_index].order_keys[0];
    let nulls_first = order.nulls_first.unwrap_or(order.descending);
    let mut sub = !following;
    let mut less = false;
    if order.descending {
        sub = !sub;
        less = true;
    }
    for (index, row) in partition_rows.iter().enumerate() {
        let key = &row.order_keys[0];
        if matches!(key, Value::Null) || matches!(current_key, Value::Null) {
            if if nulls_first {
                !matches!(key, Value::Null) || matches!(current_key, Value::Null)
            } else {
                matches!(key, Value::Null) || !matches!(current_key, Value::Null)
            } {
                return Ok(index);
            }
        } else if in_range_value(key, current_key, offset, sub, less)? {
            return Ok(index);
        }
    }
    Ok(partition_rows.len())
}

pub fn range_frame_end_from_offset(
    partition_rows: &[WindowRangeRow],
    order: WindowRangeOrder,
    row_index: usize,
    offset: &Value,
    following: bool,
) -> Result<usize, WindowFrameError> {
    let current_key = &partition_rows[row_index].order_keys[0];
    let nulls_first = order.nulls_first.unwrap_or(order.descending);
    let mut sub = !following;
    let mut less = true;
    if order.descending {
        sub = !sub;
        less = false;
    }
    for (index, row) in partition_rows.iter().enumerate() {
        let key = &row.order_keys[0];
        if matches!(key, Value::Null) || matches!(current_key, Value::Null) {
            if if nulls_first {
                !matches!(key, Value::Null)
            } else {
                !matches!(current_key, Value::Null)
            } {
                return Ok(index);
            }
        } else if !in_range_value(key, current_key, offset, sub, less)? {
            return Ok(index);
        }
    }
    Ok(partition_rows.len())
}

pub fn in_range_value(
    val: &Value,
    base: &Value,
    offset: &Value,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    match (val, base) {
        (Value::Int16(_), Value::Int16(_))
        | (Value::Int16(_), Value::Int32(_))
        | (Value::Int16(_), Value::Int64(_))
        | (Value::Int32(_), Value::Int16(_))
        | (Value::Int32(_), Value::Int32(_))
        | (Value::Int32(_), Value::Int64(_))
        | (Value::Int64(_), Value::Int16(_))
        | (Value::Int64(_), Value::Int32(_))
        | (Value::Int64(_), Value::Int64(_)) => in_range_int(
            int_value_for_range(val).expect("matched int val"),
            int_value_for_range(base).expect("matched int base"),
            int_value_for_range(offset).ok_or(WindowFrameError::UnsupportedRangeOffset)?,
            sub,
            less,
        ),
        (Value::Float64(val), Value::Float64(base)) => {
            in_range_float(*val, *base, float_offset_for_range(offset)?, sub, less)
        }
        (left, right)
            if numeric_value_for_range(left).is_some()
                && numeric_value_for_range(right).is_some() =>
        {
            in_range_numeric(
                numeric_value_for_range(left).expect("checked numeric val"),
                numeric_value_for_range(right).expect("checked numeric base"),
                numeric_value_for_range(offset).ok_or(WindowFrameError::UnsupportedRangeOffset)?,
                sub,
                less,
            )
        }
        (Value::Date(val), Value::Date(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_timestamp(
                date_as_timestamp_usecs(val.0),
                date_as_timestamp_usecs(base.0),
                *offset,
                sub,
                less,
            )
        }
        (Value::Timestamp(val), Value::Timestamp(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_timestamp(val.0, base.0, *offset, sub, less)
        }
        (Value::TimestampTz(val), Value::TimestampTz(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_timestamp(val.0, base.0, *offset, sub, less)
        }
        (Value::Time(val), Value::Time(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_time(val.0, base.0, *offset, sub, less)
        }
        (Value::TimeTz(val), Value::TimeTz(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_time(val.time.0, base.time.0, *offset, sub, less)
        }
        (Value::Interval(val), Value::Interval(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(WindowFrameError::UnsupportedRangeOffset);
            };
            in_range_interval(*val, *base, *offset, sub, less)
        }
        _ => Err(WindowFrameError::UnsupportedRangeOffset),
    }
}

fn int_value_for_range(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(value) => Some(i128::from(*value)),
        Value::Int32(value) => Some(i128::from(*value)),
        Value::Int64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn in_range_int(
    val: i128,
    base: i128,
    offset: i128,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if offset < 0 {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    let sum = if sub { base - offset } else { base + offset };
    Ok(if less { val <= sum } else { val >= sum })
}

fn float_offset_for_range(value: &Value) -> Result<f64, WindowFrameError> {
    match value {
        Value::Int16(value) => Ok(f64::from(*value)),
        Value::Int32(value) => Ok(f64::from(*value)),
        Value::Int64(value) => Ok(*value as f64),
        Value::Float64(value) => Ok(*value),
        _ => Err(WindowFrameError::UnsupportedRangeOffset),
    }
}

fn in_range_float(
    val: f64,
    base: f64,
    offset: f64,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if offset.is_nan() || offset < 0.0 {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    if val.is_nan() {
        return Ok(if base.is_nan() { true } else { !less });
    }
    if base.is_nan() {
        return Ok(less);
    }
    if offset.is_infinite() && base.is_infinite() && if sub { base > 0.0 } else { base < 0.0 } {
        return Ok(true);
    }
    let sum = if sub { base - offset } else { base + offset };
    Ok(if less { val <= sum } else { val >= sum })
}

fn numeric_value_for_range(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int32(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int64(value) => Some(NumericValue::from_i64(*value)),
        Value::Numeric(value) => Some(value.clone()),
        _ => None,
    }
}

fn numeric_is_negative(value: &NumericValue) -> bool {
    match value {
        NumericValue::Finite { coeff, .. } => coeff < &num_bigint::BigInt::from(0),
        NumericValue::NegInf => true,
        NumericValue::PosInf | NumericValue::NaN => false,
    }
}

fn in_range_numeric(
    val: NumericValue,
    base: NumericValue,
    offset: NumericValue,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if matches!(offset, NumericValue::NaN) || numeric_is_negative(&offset) {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    if matches!(val, NumericValue::NaN) {
        return Ok(if matches!(base, NumericValue::NaN) {
            true
        } else {
            !less
        });
    }
    if matches!(base, NumericValue::NaN) {
        return Ok(less);
    }
    if matches!(offset, NumericValue::PosInf) {
        if if sub {
            matches!(base, NumericValue::PosInf)
        } else {
            matches!(base, NumericValue::NegInf)
        } {
            return Ok(true);
        }
    }
    let sum = if sub {
        base.sub(&offset)
    } else {
        base.add(&offset)
    };
    Ok(if less {
        val.cmp(&sum) != Ordering::Greater
    } else {
        val.cmp(&sum) != Ordering::Less
    })
}

fn date_as_timestamp_usecs(days: i32) -> i64 {
    match days {
        DATEVAL_NOBEGIN => TIMESTAMP_NOBEGIN,
        DATEVAL_NOEND => TIMESTAMP_NOEND,
        days => {
            let usecs = i128::from(days) * i128::from(USECS_PER_DAY);
            if usecs > i128::from(TIMESTAMP_NOEND) {
                TIMESTAMP_NOEND
            } else if usecs < i128::from(TIMESTAMP_NOBEGIN) {
                TIMESTAMP_NOBEGIN
            } else {
                usecs as i64
            }
        }
    }
}

fn in_range_timestamp(
    val: i64,
    base: i64,
    offset: IntervalValue,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if offset.is_negative() {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    if offset.is_infinity()
        && if sub {
            base == TIMESTAMP_NOEND
        } else {
            base == TIMESTAMP_NOBEGIN
        }
    {
        return Ok(true);
    }
    let sum = timestamp_add_interval(base, offset, sub);
    Ok(if less { val <= sum } else { val >= sum })
}

fn timestamp_add_interval(base: i64, offset: IntervalValue, subtract: bool) -> i64 {
    if base == TIMESTAMP_NOBEGIN || base == TIMESTAMP_NOEND {
        return base;
    }
    let offset = if subtract { offset.negate() } else { offset };
    if offset.is_infinity() {
        return TIMESTAMP_NOEND;
    }
    if offset.is_neg_infinity() {
        return TIMESTAMP_NOBEGIN;
    }

    let (mut days, time_usecs) = timestamp_parts_from_usecs(base);
    if offset.months != 0 {
        days = add_months_to_days(days, offset.months).unwrap_or_else(|| {
            if offset.months.is_negative() {
                DATEVAL_NOBEGIN
            } else {
                DATEVAL_NOEND
            }
        });
    }
    let total = i128::from(days) * i128::from(USECS_PER_DAY)
        + i128::from(time_usecs)
        + i128::from(offset.days) * i128::from(USECS_PER_DAY)
        + i128::from(offset.time_micros);
    if total > i128::from(TIMESTAMP_NOEND) {
        TIMESTAMP_NOEND
    } else if total < i128::from(TIMESTAMP_NOBEGIN) {
        TIMESTAMP_NOBEGIN
    } else {
        total as i64
    }
}

fn add_months_to_days(days: i32, months: i32) -> Option<i32> {
    let (year, month, day) = ymd_from_days(days);
    let month_index = i64::from(year) * 12 + i64::from(month) - 1 + i64::from(months);
    let new_year = i32::try_from(month_index.div_euclid(12)).ok()?;
    let new_month = (month_index.rem_euclid(12) + 1) as u32;
    let new_day = day.min(days_in_month(new_year, new_month));
    days_from_ymd(new_year, new_month, new_day)
}

fn in_range_time(
    val: i64,
    base: i64,
    offset: IntervalValue,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if offset.is_negative() {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    let sum = if sub {
        i128::from(base) - i128::from(offset.time_micros)
    } else {
        let sum = i128::from(base) + i128::from(offset.time_micros);
        if sum > i128::from(i64::MAX) {
            return Ok(less);
        }
        sum
    };
    let val = i128::from(val);
    Ok(if less { val <= sum } else { val >= sum })
}

fn in_range_interval(
    val: IntervalValue,
    base: IntervalValue,
    offset: IntervalValue,
    sub: bool,
    less: bool,
) -> Result<bool, WindowFrameError> {
    if offset.is_negative() {
        return Err(WindowFrameError::InvalidRangeOffset);
    }
    if offset.is_infinity()
        && if sub {
            base.is_infinity()
        } else {
            base.is_neg_infinity()
        }
    {
        return Ok(true);
    }
    let sum = if sub {
        base.checked_sub(offset)
    } else {
        base.checked_add(offset)
    }
    .ok_or(WindowFrameError::InvalidRangeOffset)?;
    Ok(if less {
        val.cmp_key() <= sum.cmp_key()
    } else {
        val.cmp_key() >= sum.cmp_key()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::datetime::{DateADT, TimestampADT};

    #[test]
    fn rows_bounds_are_clamped() {
        assert_eq!(rows_frame_start(5, 1, 3, false), 0);
        assert_eq!(rows_frame_start(5, 3, 3, true), 5);
        assert_eq!(rows_frame_end(5, 1, 3, false), 0);
        assert_eq!(rows_frame_end(5, 3, 3, true), 5);
    }

    #[test]
    fn int_range_start_respects_descending_order() {
        let rows = [1, 2, 3, 4]
            .into_iter()
            .map(|value| WindowRangeRow {
                order_keys: vec![Value::Int32(value)],
            })
            .collect::<Vec<_>>();
        let ascending = WindowRangeOrder {
            descending: false,
            nulls_first: None,
        };
        let descending = WindowRangeOrder {
            descending: true,
            nulls_first: None,
        };

        assert_eq!(
            range_frame_start_from_offset(&rows, ascending, 2, &Value::Int32(1), false).unwrap(),
            1
        );
        let descending_rows = [4, 3, 2, 1]
            .into_iter()
            .map(|value| WindowRangeRow {
                order_keys: vec![Value::Int32(value)],
            })
            .collect::<Vec<_>>();
        assert_eq!(
            range_frame_start_from_offset(&descending_rows, descending, 1, &Value::Int32(1), false)
                .unwrap(),
            0
        );
    }

    #[test]
    fn negative_range_offset_is_invalid() {
        let err = in_range_value(
            &Value::Int32(1),
            &Value::Int32(2),
            &Value::Int32(-1),
            true,
            false,
        )
        .unwrap_err();

        assert_eq!(err, WindowFrameError::InvalidRangeOffset);
    }

    #[test]
    fn date_range_uses_interval_offsets() {
        let offset = Value::Interval(IntervalValue {
            time_micros: 0,
            days: 2,
            months: 0,
        });

        assert!(
            in_range_value(
                &Value::Date(DateADT(8)),
                &Value::Date(DateADT(10)),
                &offset,
                true,
                false,
            )
            .unwrap()
        );
        assert!(
            !in_range_value(
                &Value::Date(DateADT(7)),
                &Value::Date(DateADT(10)),
                &offset,
                true,
                false,
            )
            .unwrap()
        );
    }

    #[test]
    fn timestamp_month_offsets_clamp_to_last_day() {
        let jan_31 = pgrust_expr::utils::time::datetime::days_from_ymd(2024, 1, 31)
            .map(|days| i64::from(days) * USECS_PER_DAY)
            .unwrap();
        let feb_29 = pgrust_expr::utils::time::datetime::days_from_ymd(2024, 2, 29)
            .map(|days| i64::from(days) * USECS_PER_DAY)
            .unwrap();
        let offset = Value::Interval(IntervalValue {
            time_micros: 0,
            days: 0,
            months: 1,
        });

        assert!(
            in_range_value(
                &Value::Timestamp(TimestampADT(feb_29)),
                &Value::Timestamp(TimestampADT(jan_31)),
                &offset,
                false,
                true,
            )
            .unwrap()
        );
    }
}
