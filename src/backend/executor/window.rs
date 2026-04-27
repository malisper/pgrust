use super::agg::AccumState;
use super::exec_expr::eval_expr;
use super::expr_ops::{compare_order_values, values_are_distinct};
use super::{ExecError, ExecutorContext};
use crate::backend::utils::time::datetime::{
    days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, USECS_PER_DAY,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue, Value};
use crate::include::nodes::execnodes::{MaterializedRow, SystemVarBinding, TupleSlot};
use crate::include::nodes::parsenodes::{WindowFrameExclusion, WindowFrameMode};
use crate::include::nodes::primnodes::{
    AggFunc, BuiltinWindowFunction, OrderByEntry, WindowClause, WindowFrameBound, WindowFuncExpr,
    WindowFuncKind,
};
use std::cmp::Ordering;

const INVALID_PARAMETER_VALUE_SQLSTATE: &str = "22023";

#[derive(Debug)]
struct PreparedWindowRow {
    row: MaterializedRow,
    partition_keys: Vec<Value>,
    order_keys: Vec<Value>,
}

fn set_active_system_bindings(ctx: &mut ExecutorContext, bindings: &[SystemVarBinding]) {
    ctx.system_bindings.clear();
    ctx.system_bindings.extend_from_slice(bindings);
}

fn set_outer_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.outer_tuple = Some(values);
    ctx.expr_bindings.outer_system_bindings = bindings.to_vec();
    ctx.expr_bindings.inner_tuple = None;
    ctx.expr_bindings.inner_system_bindings.clear();
}

fn prepare_input_rows(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    rows: Vec<MaterializedRow>,
) -> Result<Vec<PreparedWindowRow>, ExecError> {
    rows.into_iter()
        .map(|mut row| {
            set_active_system_bindings(ctx, &row.system_bindings);
            set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
            let partition_keys = clause
                .spec
                .partition_by
                .iter()
                .map(|expr| eval_expr(expr, &mut row.slot, ctx).map(|value| value.to_owned_value()))
                .collect::<Result<Vec<_>, _>>()?;
            let order_keys = clause
                .spec
                .order_by
                .iter()
                .map(|item| {
                    eval_expr(&item.expr, &mut row.slot, ctx).map(|value| value.to_owned_value())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PreparedWindowRow {
                row,
                partition_keys,
                order_keys,
            })
        })
        .collect()
}

fn same_partition(left: &PreparedWindowRow, right: &PreparedWindowRow) -> bool {
    left.partition_keys.len() == right.partition_keys.len()
        && left
            .partition_keys
            .iter()
            .zip(right.partition_keys.iter())
            .all(|(left, right)| !values_are_distinct(left, right))
}

fn same_peer(
    order_by: &[OrderByEntry],
    left: &PreparedWindowRow,
    right: &PreparedWindowRow,
) -> Result<bool, ExecError> {
    if left.order_keys.len() != right.order_keys.len() || left.order_keys.len() != order_by.len() {
        return Ok(false);
    }
    for (item, (left, right)) in order_by
        .iter()
        .zip(left.order_keys.iter().zip(right.order_keys.iter()))
    {
        if compare_order_values(left, right, item.collation_oid, None, false)? != Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn evaluate_window_expr_on_row(
    ctx: &mut ExecutorContext,
    row: &mut PreparedWindowRow,
    expr: &crate::include::nodes::primnodes::Expr,
) -> Result<Value, ExecError> {
    set_active_system_bindings(ctx, &row.row.system_bindings);
    set_outer_expr_bindings(
        ctx,
        row.row.slot.tts_values.clone(),
        &row.row.system_bindings,
    );
    eval_expr(expr, &mut row.row.slot, ctx).map(|value| value.to_owned_value())
}

fn peer_group_end_for_index(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    index: usize,
) -> Result<usize, ExecError> {
    let mut peer_end = index + 1;
    while peer_end < partition_rows.len()
        && same_peer(
            order_by,
            &partition_rows[peer_end - 1],
            &partition_rows[peer_end],
        )?
    {
        peer_end += 1;
    }
    Ok(peer_end)
}

fn peer_group_start_for_index(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    index: usize,
) -> Result<usize, ExecError> {
    let mut peer_start = index;
    while peer_start > 0
        && same_peer(
            order_by,
            &partition_rows[peer_start - 1],
            &partition_rows[peer_start],
        )?
    {
        peer_start -= 1;
    }
    Ok(peer_start)
}

fn current_row_frame_error(which: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: format!("frame {which} offset must not be null"),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn negative_frame_error(which: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: format!("frame {which} offset must not be negative"),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn unsupported_range_offset_error() -> ExecError {
    ExecError::DetailedError {
        message: "RANGE with offset PRECEDING/FOLLOWING is not supported for this ORDER BY type"
            .into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn invalid_range_offset_error() -> ExecError {
    ExecError::DetailedError {
        message: "invalid preceding or following size in window function".into(),
        detail: None,
        hint: None,
        sqlstate: "22013",
    }
}

fn evaluate_range_frame_offset_value(
    ctx: &mut ExecutorContext,
    current_row: &mut PreparedWindowRow,
    offset: &crate::include::nodes::primnodes::WindowFrameOffset,
    which: &'static str,
) -> Result<Value, ExecError> {
    let value = evaluate_window_expr_on_row(ctx, current_row, &offset.expr)?;
    if matches!(value, Value::Null) {
        return Err(current_row_frame_error(which));
    }
    Ok(value)
}

fn evaluate_frame_bound_i64(
    ctx: &mut ExecutorContext,
    current_row: &mut PreparedWindowRow,
    bound: &WindowFrameBound,
    which: &'static str,
) -> Result<Option<i64>, ExecError> {
    let offset = match bound {
        WindowFrameBound::OffsetPreceding(offset) | WindowFrameBound::OffsetFollowing(offset) => {
            offset
        }
        _ => return Ok(None),
    };
    let value = evaluate_window_expr_on_row(ctx, current_row, &offset.expr)?;
    if matches!(value, Value::Null) {
        return Err(current_row_frame_error(which));
    }
    let offset = match value {
        Value::Int16(value) => i64::from(value),
        Value::Int32(value) => i64::from(value),
        Value::Int64(value) => value,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "window frame offset",
                left: other,
                right: Value::Int32(1),
            });
        }
    };
    if offset < 0 {
        return Err(negative_frame_error(which));
    }
    Ok(Some(offset))
}

fn move_group_start(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    row_index: usize,
    offset: i64,
    following: bool,
) -> Result<usize, ExecError> {
    let mut start = peer_group_start_for_index(partition_rows, order_by, row_index)?;
    let mut remaining = offset;
    while remaining > 0 {
        if following {
            let end = peer_group_end_for_index(partition_rows, order_by, start)?;
            if end >= partition_rows.len() {
                return Ok(partition_rows.len());
            }
            start = end;
        } else if start == 0 {
            return Ok(0);
        } else {
            start = peer_group_start_for_index(partition_rows, order_by, start - 1)?;
        }
        remaining -= 1;
    }
    Ok(start)
}

fn move_group_end(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    row_index: usize,
    offset: i64,
    following: bool,
) -> Result<usize, ExecError> {
    let start = move_group_start(partition_rows, order_by, row_index, offset, following)?;
    if start >= partition_rows.len() {
        Ok(partition_rows.len())
    } else {
        peer_group_end_for_index(partition_rows, order_by, start)
    }
}

fn rows_frame_start(len: usize, row_index: usize, offset: i64, following: bool) -> usize {
    if following {
        row_index.saturating_add(offset as usize).min(len)
    } else {
        row_index.saturating_sub(offset as usize)
    }
}

fn rows_frame_end(len: usize, row_index: usize, offset: i64, following: bool) -> usize {
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

fn range_frame_start_from_offset(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    row_index: usize,
    offset: &Value,
    following: bool,
) -> Result<usize, ExecError> {
    let item = &order_by[0];
    let current_key = &partition_rows[row_index].order_keys[0];
    let nulls_first = item.nulls_first.unwrap_or(item.descending);
    let mut sub = !following;
    let mut less = false;
    if item.descending {
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

fn range_frame_end_from_offset(
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
    row_index: usize,
    offset: &Value,
    following: bool,
) -> Result<usize, ExecError> {
    let item = &order_by[0];
    let current_key = &partition_rows[row_index].order_keys[0];
    let nulls_first = item.nulls_first.unwrap_or(item.descending);
    let mut sub = !following;
    let mut less = true;
    if item.descending {
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

fn in_range_value(
    val: &Value,
    base: &Value,
    offset: &Value,
    sub: bool,
    less: bool,
) -> Result<bool, ExecError> {
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
            int_value_for_range(offset).ok_or_else(unsupported_range_offset_error)?,
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
                numeric_value_for_range(offset).ok_or_else(unsupported_range_offset_error)?,
                sub,
                less,
            )
        }
        (Value::Date(val), Value::Date(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(unsupported_range_offset_error());
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
                return Err(unsupported_range_offset_error());
            };
            in_range_timestamp(val.0, base.0, *offset, sub, less)
        }
        (Value::TimestampTz(val), Value::TimestampTz(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(unsupported_range_offset_error());
            };
            in_range_timestamp(val.0, base.0, *offset, sub, less)
        }
        (Value::Time(val), Value::Time(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(unsupported_range_offset_error());
            };
            in_range_time(val.0, base.0, *offset, sub, less)
        }
        (Value::TimeTz(val), Value::TimeTz(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(unsupported_range_offset_error());
            };
            in_range_time(val.time.0, base.time.0, *offset, sub, less)
        }
        (Value::Interval(val), Value::Interval(base)) => {
            let Value::Interval(offset) = offset else {
                return Err(unsupported_range_offset_error());
            };
            in_range_interval(*val, *base, *offset, sub, less)
        }
        _ => Err(unsupported_range_offset_error()),
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
) -> Result<bool, ExecError> {
    if offset < 0 {
        return Err(invalid_range_offset_error());
    }
    let sum = if sub { base - offset } else { base + offset };
    Ok(if less { val <= sum } else { val >= sum })
}

fn float_offset_for_range(value: &Value) -> Result<f64, ExecError> {
    match value {
        Value::Int16(value) => Ok(f64::from(*value)),
        Value::Int32(value) => Ok(f64::from(*value)),
        Value::Int64(value) => Ok(*value as f64),
        Value::Float64(value) => Ok(*value),
        _ => Err(unsupported_range_offset_error()),
    }
}

fn in_range_float(
    val: f64,
    base: f64,
    offset: f64,
    sub: bool,
    less: bool,
) -> Result<bool, ExecError> {
    if offset.is_nan() || offset < 0.0 {
        return Err(invalid_range_offset_error());
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
) -> Result<bool, ExecError> {
    if matches!(offset, NumericValue::NaN) || numeric_is_negative(&offset) {
        return Err(invalid_range_offset_error());
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
) -> Result<bool, ExecError> {
    if offset.is_negative() {
        return Err(invalid_range_offset_error());
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
) -> Result<bool, ExecError> {
    if offset.is_negative() {
        return Err(invalid_range_offset_error());
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
) -> Result<bool, ExecError> {
    if offset.is_negative() {
        return Err(invalid_range_offset_error());
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
    .ok_or_else(invalid_range_offset_error)?;
    Ok(if less {
        val.cmp_key() <= sum.cmp_key()
    } else {
        val.cmp_key() >= sum.cmp_key()
    })
}

fn evaluate_window_frame(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
) -> Result<(usize, usize), ExecError> {
    let frame = &clause.spec.frame;
    let len = partition_rows.len();
    let peer_start = peer_group_start_for_index(partition_rows, &clause.spec.order_by, row_index)?;
    let peer_end = peer_group_end_for_index(partition_rows, &clause.spec.order_by, row_index)?;

    let start = match (&frame.mode, &frame.start_bound) {
        (_, WindowFrameBound::UnboundedPreceding) => 0,
        (_, WindowFrameBound::CurrentRow) => match frame.mode {
            WindowFrameMode::Rows => row_index,
            WindowFrameMode::Range | WindowFrameMode::Groups => peer_start,
        },
        (_, WindowFrameBound::UnboundedFollowing) => len,
        (WindowFrameMode::Rows, WindowFrameBound::OffsetPreceding(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.start_bound,
                "starting",
            )?
            .expect("offset");
            rows_frame_start(len, row_index, offset, false)
        }
        (WindowFrameMode::Rows, WindowFrameBound::OffsetFollowing(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.start_bound,
                "starting",
            )?
            .expect("offset");
            rows_frame_start(len, row_index, offset, true)
        }
        (WindowFrameMode::Groups, WindowFrameBound::OffsetPreceding(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.start_bound,
                "starting",
            )?
            .expect("offset");
            move_group_start(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                offset,
                false,
            )?
        }
        (WindowFrameMode::Groups, WindowFrameBound::OffsetFollowing(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.start_bound,
                "starting",
            )?
            .expect("offset");
            move_group_start(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                offset,
                true,
            )?
        }
        (WindowFrameMode::Range, WindowFrameBound::OffsetPreceding(offset))
        | (WindowFrameMode::Range, WindowFrameBound::OffsetFollowing(offset)) => {
            let offset = evaluate_range_frame_offset_value(
                ctx,
                &mut partition_rows[row_index],
                offset,
                "starting",
            )?;
            range_frame_start_from_offset(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                &offset,
                matches!(&frame.start_bound, WindowFrameBound::OffsetFollowing(_)),
            )?
        }
    };

    let end = match (&frame.mode, &frame.end_bound) {
        (_, WindowFrameBound::UnboundedFollowing) => len,
        (_, WindowFrameBound::CurrentRow) => match frame.mode {
            WindowFrameMode::Rows => row_index + 1,
            WindowFrameMode::Range | WindowFrameMode::Groups => peer_end,
        },
        (_, WindowFrameBound::UnboundedPreceding) => 0,
        (WindowFrameMode::Rows, WindowFrameBound::OffsetPreceding(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.end_bound,
                "ending",
            )?
            .expect("offset");
            rows_frame_end(len, row_index, offset, false)
        }
        (WindowFrameMode::Rows, WindowFrameBound::OffsetFollowing(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.end_bound,
                "ending",
            )?
            .expect("offset");
            rows_frame_end(len, row_index, offset, true)
        }
        (WindowFrameMode::Groups, WindowFrameBound::OffsetPreceding(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.end_bound,
                "ending",
            )?
            .expect("offset");
            move_group_end(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                offset,
                false,
            )?
        }
        (WindowFrameMode::Groups, WindowFrameBound::OffsetFollowing(_)) => {
            let offset = evaluate_frame_bound_i64(
                ctx,
                &mut partition_rows[row_index],
                &frame.end_bound,
                "ending",
            )?
            .expect("offset");
            move_group_end(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                offset,
                true,
            )?
        }
        (WindowFrameMode::Range, WindowFrameBound::OffsetPreceding(offset))
        | (WindowFrameMode::Range, WindowFrameBound::OffsetFollowing(offset)) => {
            let offset = evaluate_range_frame_offset_value(
                ctx,
                &mut partition_rows[row_index],
                offset,
                "ending",
            )?;
            range_frame_end_from_offset(
                partition_rows,
                &clause.spec.order_by,
                row_index,
                &offset,
                matches!(&frame.end_bound, WindowFrameBound::OffsetFollowing(_)),
            )?
        }
    };

    Ok(if start >= end {
        (start, start)
    } else {
        (start, end)
    })
}

fn row_is_included_by_frame_exclusion(
    clause: &WindowClause,
    partition_rows: &[PreparedWindowRow],
    row_index: usize,
    candidate_index: usize,
) -> Result<bool, ExecError> {
    match clause.spec.frame.exclusion {
        WindowFrameExclusion::NoOthers => Ok(true),
        WindowFrameExclusion::CurrentRow => Ok(candidate_index != row_index),
        WindowFrameExclusion::Group => Ok(!same_peer(
            &clause.spec.order_by,
            &partition_rows[row_index],
            &partition_rows[candidate_index],
        )?),
        WindowFrameExclusion::Ties => {
            if candidate_index == row_index {
                return Ok(true);
            }
            Ok(!same_peer(
                &clause.spec.order_by,
                &partition_rows[row_index],
                &partition_rows[candidate_index],
            )?)
        }
    }
}

fn first_included_frame_row_index(
    clause: &WindowClause,
    partition_rows: &[PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
) -> Result<Option<usize>, ExecError> {
    for candidate in frame_start..frame_end {
        if row_is_included_by_frame_exclusion(clause, partition_rows, row_index, candidate)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn last_included_frame_row_index(
    clause: &WindowClause,
    partition_rows: &[PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
) -> Result<Option<usize>, ExecError> {
    for candidate in (frame_start..frame_end).rev() {
        if row_is_included_by_frame_exclusion(clause, partition_rows, row_index, candidate)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn nth_included_frame_row_index(
    clause: &WindowClause,
    partition_rows: &[PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
    nth: usize,
) -> Result<Option<usize>, ExecError> {
    let mut seen = 0usize;
    for candidate in frame_start..frame_end {
        if row_is_included_by_frame_exclusion(clause, partition_rows, row_index, candidate)? {
            seen += 1;
            if seen == nth {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

fn included_frame_value_is_non_null(
    ctx: &mut ExecutorContext,
    value_expr: &crate::include::nodes::primnodes::Expr,
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
    candidate: usize,
) -> Result<bool, ExecError> {
    if !row_is_included_by_frame_exclusion(clause, partition_rows, row_index, candidate)? {
        return Ok(false);
    }
    let value = evaluate_window_expr_on_row(ctx, &mut partition_rows[candidate], value_expr)?;
    Ok(!matches!(value, Value::Null))
}

fn first_non_null_included_frame_row_index(
    ctx: &mut ExecutorContext,
    value_expr: &crate::include::nodes::primnodes::Expr,
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
) -> Result<Option<usize>, ExecError> {
    for candidate in frame_start..frame_end {
        if included_frame_value_is_non_null(
            ctx,
            value_expr,
            clause,
            partition_rows,
            row_index,
            candidate,
        )? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn last_non_null_included_frame_row_index(
    ctx: &mut ExecutorContext,
    value_expr: &crate::include::nodes::primnodes::Expr,
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
) -> Result<Option<usize>, ExecError> {
    for candidate in (frame_start..frame_end).rev() {
        if included_frame_value_is_non_null(
            ctx,
            value_expr,
            clause,
            partition_rows,
            row_index,
            candidate,
        )? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn nth_non_null_included_frame_row_index(
    ctx: &mut ExecutorContext,
    value_expr: &crate::include::nodes::primnodes::Expr,
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
    frame_start: usize,
    frame_end: usize,
    nth: usize,
) -> Result<Option<usize>, ExecError> {
    let mut seen = 0usize;
    for candidate in frame_start..frame_end {
        if included_frame_value_is_non_null(
            ctx,
            value_expr,
            clause,
            partition_rows,
            row_index,
            candidate,
        )? {
            seen += 1;
            if seen == nth {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

fn advance_window_aggregate(
    ctx: &mut ExecutorContext,
    state: &mut AccumState,
    row: &mut MaterializedRow,
    aggref: &crate::include::nodes::primnodes::Aggref,
) -> Result<(), ExecError> {
    set_active_system_bindings(ctx, &row.system_bindings);
    set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
    if let Some(filter) = aggref.aggfilter.as_ref() {
        match eval_expr(filter, &mut row.slot, ctx)? {
            Value::Bool(true) => {}
            Value::Bool(false) | Value::Null => return Ok(()),
            other => return Err(ExecError::NonBoolQual(other)),
        }
    }
    let values = aggref
        .args
        .iter()
        .map(|arg| eval_expr(arg, &mut row.slot, ctx).map(|value| value.to_owned_value()))
        .collect::<Result<Vec<_>, _>>()?;
    let func = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid).unwrap_or_else(|| {
        panic!(
            "window aggregate {:?} lacks builtin implementation mapping",
            aggref.aggfnoid
        )
    });
    let transition = AccumState::transition_fn(func, aggref.args.len(), aggref.aggdistinct);
    transition(state, &values)?;
    Ok(())
}

fn evaluate_rank_like_window(
    func: BuiltinWindowFunction,
    partition_rows: &[PreparedWindowRow],
    order_by: &[OrderByEntry],
) -> Result<Vec<Value>, ExecError> {
    let mut values = Vec::with_capacity(partition_rows.len());
    let mut dense_rank = 1i64;
    let total_rows = partition_rows.len();
    let mut peer_start = 0usize;
    while peer_start < total_rows {
        let mut peer_end = peer_start + 1;
        while peer_end < total_rows
            && same_peer(
                order_by,
                &partition_rows[peer_end - 1],
                &partition_rows[peer_end],
            )?
        {
            peer_end += 1;
        }

        match func {
            BuiltinWindowFunction::RowNumber => {
                for index in peer_start..peer_end {
                    values.push(Value::Int64(index as i64 + 1));
                }
            }
            BuiltinWindowFunction::Rank => {
                values.extend(std::iter::repeat_n(
                    Value::Int64(peer_start as i64 + 1),
                    peer_end - peer_start,
                ));
            }
            BuiltinWindowFunction::DenseRank => {
                values.extend(std::iter::repeat_n(
                    Value::Int64(dense_rank),
                    peer_end - peer_start,
                ));
            }
            BuiltinWindowFunction::PercentRank => {
                let percent_rank = if total_rows <= 1 {
                    0.0
                } else {
                    peer_start as f64 / (total_rows - 1) as f64
                };
                values.extend(std::iter::repeat_n(
                    Value::Float64(percent_rank),
                    peer_end - peer_start,
                ));
            }
            BuiltinWindowFunction::CumeDist => {
                let cume_dist = peer_end as f64 / total_rows as f64;
                values.extend(std::iter::repeat_n(
                    Value::Float64(cume_dist),
                    peer_end - peer_start,
                ));
            }
            BuiltinWindowFunction::Ntile => {
                panic!("ntile() must be evaluated through evaluate_ntile_window")
            }
            BuiltinWindowFunction::Lag | BuiltinWindowFunction::Lead => {
                panic!("lag/lead must be evaluated through evaluate_offset_window")
            }
            BuiltinWindowFunction::FirstValue
            | BuiltinWindowFunction::LastValue
            | BuiltinWindowFunction::NthValue => {
                panic!("value window functions must be evaluated through evaluate_value_window")
            }
        }

        peer_start = peer_end;
        dense_rank += 1;
    }
    Ok(values)
}

fn invalid_ntile_bucket_error() -> ExecError {
    ExecError::DetailedError {
        message: "argument of ntile must be greater than zero".into(),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn invalid_nth_value_argument_error() -> ExecError {
    ExecError::DetailedError {
        message: "argument of nth_value must be greater than zero".into(),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn evaluate_ntile_bucket_count(
    ctx: &mut ExecutorContext,
    func: &WindowFuncExpr,
    partition_rows: &mut [PreparedWindowRow],
) -> Result<Option<usize>, ExecError> {
    let Some(first_row) = partition_rows.first_mut() else {
        return Ok(Some(1));
    };
    let Some(bucket_arg) = func.args.first() else {
        panic!("ntile() missing bucket-count argument");
    };

    set_active_system_bindings(ctx, &first_row.row.system_bindings);
    set_outer_expr_bindings(
        ctx,
        first_row.row.slot.tts_values.clone(),
        &first_row.row.system_bindings,
    );
    let bucket_count = eval_expr(bucket_arg, &mut first_row.row.slot, ctx)?.to_owned_value();
    match bucket_count {
        Value::Null => Ok(None),
        Value::Int16(value) if value > 0 => Ok(Some(value as usize)),
        Value::Int32(value) if value > 0 => Ok(Some(value as usize)),
        Value::Int64(value) if value > 0 => usize::try_from(value)
            .map(Some)
            .map_err(|_| invalid_ntile_bucket_error()),
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => Err(invalid_ntile_bucket_error()),
        other => Err(ExecError::TypeMismatch {
            op: "ntile",
            left: other,
            right: Value::Int32(1),
        }),
    }
}

fn evaluate_ntile_window(
    ctx: &mut ExecutorContext,
    func: &WindowFuncExpr,
    partition_rows: &mut [PreparedWindowRow],
) -> Result<Vec<Value>, ExecError> {
    let total_rows = partition_rows.len();
    if total_rows == 0 {
        return Ok(Vec::new());
    }

    let Some(bucket_count) = evaluate_ntile_bucket_count(ctx, func, partition_rows)? else {
        return Ok(vec![Value::Null; total_rows]);
    };

    if total_rows < bucket_count {
        return (1..=total_rows)
            .map(|bucket| {
                i32::try_from(bucket)
                    .map(Value::Int32)
                    .map_err(|_| invalid_ntile_bucket_error())
            })
            .collect();
    }

    let rows_per_bucket = total_rows / bucket_count;
    let remainder = total_rows % bucket_count;
    let mut values = Vec::with_capacity(total_rows);
    for bucket_index in 0..bucket_count {
        let bucket_size = rows_per_bucket + usize::from(bucket_index < remainder);
        let bucket_value =
            i32::try_from(bucket_index + 1).map_err(|_| invalid_ntile_bucket_error())?;
        values.extend(std::iter::repeat_n(Value::Int32(bucket_value), bucket_size));
        if values.len() == total_rows {
            break;
        }
    }
    Ok(values)
}

fn evaluate_nth_value_offset(
    ctx: &mut ExecutorContext,
    func: &WindowFuncExpr,
    current_row: &mut PreparedWindowRow,
) -> Result<Option<usize>, ExecError> {
    let Some(offset_expr) = func.args.get(1) else {
        panic!("nth_value() missing offset argument");
    };
    match evaluate_window_expr_on_row(ctx, current_row, offset_expr)? {
        Value::Null => Ok(None),
        Value::Int16(value) if value > 0 => Ok(Some(value as usize)),
        Value::Int32(value) if value > 0 => Ok(Some(value as usize)),
        Value::Int64(value) if value > 0 => usize::try_from(value)
            .map(Some)
            .map_err(|_| invalid_nth_value_argument_error()),
        Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => {
            Err(invalid_nth_value_argument_error())
        }
        other => Err(ExecError::TypeMismatch {
            op: "nth_value",
            left: other,
            right: Value::Int32(1),
        }),
    }
}

fn evaluate_lag_lead_offset(
    ctx: &mut ExecutorContext,
    func: &WindowFuncExpr,
    current_row: &mut PreparedWindowRow,
) -> Result<Option<i64>, ExecError> {
    let Some(offset_expr) = func.args.get(1) else {
        return Ok(Some(1));
    };
    match evaluate_window_expr_on_row(ctx, current_row, offset_expr)? {
        Value::Null => Ok(None),
        Value::Int16(value) => Ok(Some(i64::from(value))),
        Value::Int32(value) => Ok(Some(i64::from(value))),
        Value::Int64(value) => Ok(Some(value)),
        other => Err(ExecError::TypeMismatch {
            op: "lag/lead offset",
            left: other,
            right: Value::Int32(1),
        }),
    }
}

fn evaluate_offset_window(
    ctx: &mut ExecutorContext,
    func: &WindowFuncExpr,
    partition_rows: &mut [PreparedWindowRow],
) -> Result<Vec<Value>, ExecError> {
    let Some(value_expr) = func.args.first() else {
        panic!("lag/lead missing value argument");
    };
    let builtin = match &func.kind {
        WindowFuncKind::Builtin(kind) => *kind,
        WindowFuncKind::Aggregate(_) => panic!("aggregate window function routed to lag/lead path"),
    };

    let mut values = Vec::with_capacity(partition_rows.len());
    for row_index in 0..partition_rows.len() {
        let Some(offset) = evaluate_lag_lead_offset(ctx, func, &mut partition_rows[row_index])?
        else {
            values.push(Value::Null);
            continue;
        };

        let direction = match builtin {
            BuiltinWindowFunction::Lag => -i128::from(offset),
            BuiltinWindowFunction::Lead => i128::from(offset),
            _ => panic!("non-offset window function routed to lag/lead path"),
        };
        let target_index = if func.ignore_nulls {
            offset_window_ignore_nulls_target_index(
                ctx,
                value_expr,
                partition_rows,
                row_index,
                direction,
            )?
        } else {
            let target_index = row_index as i128 + direction;
            (target_index >= 0 && target_index < partition_rows.len() as i128)
                .then_some(target_index as usize)
        };
        let value = match target_index {
            Some(target_index) => {
                evaluate_window_expr_on_row(ctx, &mut partition_rows[target_index], value_expr)?
            }
            None => {
                if let Some(default_expr) = func.args.get(2) {
                    evaluate_window_expr_on_row(ctx, &mut partition_rows[row_index], default_expr)?
                } else {
                    Value::Null
                }
            }
        };
        values.push(value);
    }
    Ok(values)
}

fn offset_window_ignore_nulls_target_index(
    ctx: &mut ExecutorContext,
    value_expr: &crate::include::nodes::primnodes::Expr,
    partition_rows: &mut [PreparedWindowRow],
    row_index: usize,
    direction: i128,
) -> Result<Option<usize>, ExecError> {
    if direction == 0 {
        let value = evaluate_window_expr_on_row(ctx, &mut partition_rows[row_index], value_expr)?;
        return Ok((!matches!(value, Value::Null)).then_some(row_index));
    }

    let step = direction.signum();
    let mut remaining = direction.unsigned_abs();
    let mut candidate = row_index as i128;
    while remaining > 0 {
        candidate += step;
        if candidate < 0 || candidate >= partition_rows.len() as i128 {
            return Ok(None);
        }
        let value =
            evaluate_window_expr_on_row(ctx, &mut partition_rows[candidate as usize], value_expr)?;
        if !matches!(value, Value::Null) {
            remaining -= 1;
        }
    }
    Ok(Some(candidate as usize))
}

fn evaluate_value_window(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    func: &WindowFuncExpr,
    partition_rows: &mut [PreparedWindowRow],
) -> Result<Vec<Value>, ExecError> {
    let Some(value_expr) = func.args.first() else {
        panic!("value window function missing value argument");
    };
    let builtin = match &func.kind {
        WindowFuncKind::Builtin(kind) => *kind,
        WindowFuncKind::Aggregate(_) => panic!("aggregate window function routed to value path"),
    };
    let mut values = Vec::with_capacity(partition_rows.len());
    for row_index in 0..partition_rows.len() {
        let (frame_start, frame_end) =
            evaluate_window_frame(ctx, clause, partition_rows, row_index)?;
        let frame_row_index = match builtin {
            BuiltinWindowFunction::FirstValue if func.ignore_nulls => {
                first_non_null_included_frame_row_index(
                    ctx,
                    value_expr,
                    clause,
                    partition_rows,
                    row_index,
                    frame_start,
                    frame_end,
                )?
            }
            BuiltinWindowFunction::FirstValue => first_included_frame_row_index(
                clause,
                partition_rows,
                row_index,
                frame_start,
                frame_end,
            )?,
            BuiltinWindowFunction::LastValue if func.ignore_nulls => {
                last_non_null_included_frame_row_index(
                    ctx,
                    value_expr,
                    clause,
                    partition_rows,
                    row_index,
                    frame_start,
                    frame_end,
                )?
            }
            BuiltinWindowFunction::LastValue => last_included_frame_row_index(
                clause,
                partition_rows,
                row_index,
                frame_start,
                frame_end,
            )?,
            BuiltinWindowFunction::NthValue => {
                let Some(offset) =
                    evaluate_nth_value_offset(ctx, func, &mut partition_rows[row_index])?
                else {
                    values.push(Value::Null);
                    continue;
                };
                if func.ignore_nulls {
                    nth_non_null_included_frame_row_index(
                        ctx,
                        value_expr,
                        clause,
                        partition_rows,
                        row_index,
                        frame_start,
                        frame_end,
                        offset,
                    )?
                } else {
                    nth_included_frame_row_index(
                        clause,
                        partition_rows,
                        row_index,
                        frame_start,
                        frame_end,
                        offset,
                    )?
                }
            }
            _ => panic!("non-value window function routed to value path"),
        };

        let value = match frame_row_index {
            Some(index) => {
                evaluate_window_expr_on_row(ctx, &mut partition_rows[index], value_expr)?
            }
            None => Value::Null,
        };
        values.push(value);
    }
    Ok(values)
}

fn evaluate_builtin_window(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    func: &WindowFuncExpr,
    partition_rows: &mut [PreparedWindowRow],
) -> Result<Vec<Value>, ExecError> {
    let builtin = match &func.kind {
        WindowFuncKind::Builtin(kind) => *kind,
        WindowFuncKind::Aggregate(_) => panic!("aggregate window function routed to builtin path"),
    };
    match builtin {
        BuiltinWindowFunction::Ntile => evaluate_ntile_window(ctx, func, partition_rows),
        BuiltinWindowFunction::Lag | BuiltinWindowFunction::Lead => {
            evaluate_offset_window(ctx, func, partition_rows)
        }
        BuiltinWindowFunction::FirstValue
        | BuiltinWindowFunction::LastValue
        | BuiltinWindowFunction::NthValue => {
            evaluate_value_window(ctx, clause, func, partition_rows)
        }
        _ => evaluate_rank_like_window(builtin, partition_rows, &clause.spec.order_by),
    }
}

fn evaluate_aggregate_window(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    aggref: &crate::include::nodes::primnodes::Aggref,
    partition_rows: &mut [PreparedWindowRow],
    has_order_by: bool,
) -> Result<Vec<Value>, ExecError> {
    let func = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid).unwrap_or_else(|| {
        panic!(
            "window aggregate {:?} lacks builtin implementation mapping",
            aggref.aggfnoid
        )
    });
    let mut state = AccumState::new(func, aggref.aggdistinct, aggref.aggtype);
    if !has_order_by
        && matches!(clause.spec.frame.mode, WindowFrameMode::Range)
        && clause.spec.frame.exclusion == WindowFrameExclusion::NoOthers
    {
        for row in partition_rows.iter_mut() {
            advance_window_aggregate(ctx, &mut state, &mut row.row, aggref)?;
        }
        return Ok(vec![state.finalize(); partition_rows.len()]);
    }

    if let Some(values) =
        evaluate_peer_prefix_aggregate_window(ctx, clause, aggref, partition_rows, func)?
    {
        return Ok(values);
    }

    // PostgreSQL advances aggregate state incrementally for nonshrinking frames.
    // Match that behavior for prefix-style frames so running totals do not
    // devolve into per-row full-frame rescans.
    if frame_uses_prefix_accumulation(clause, partition_rows, ctx, aggref)? {
        let mut values = Vec::with_capacity(partition_rows.len());
        let mut state = AccumState::new(func, aggref.aggdistinct, aggref.aggtype);
        let mut advanced_end = 0usize;
        for row_index in 0..partition_rows.len() {
            let (_, frame_end) = evaluate_window_frame(ctx, clause, partition_rows, row_index)?;
            while advanced_end < frame_end {
                advance_window_aggregate(
                    ctx,
                    &mut state,
                    &mut partition_rows[advanced_end].row,
                    aggref,
                )?;
                advanced_end += 1;
            }
            values.push(state.finalize());
        }
        return Ok(values);
    }

    let mut values = Vec::with_capacity(partition_rows.len());
    for row_index in 0..partition_rows.len() {
        let (frame_start, frame_end) =
            evaluate_window_frame(ctx, clause, partition_rows, row_index)?;
        let mut state = AccumState::new(func, aggref.aggdistinct, aggref.aggtype);
        let included_indexes = (frame_start..frame_end)
            .map(|candidate| {
                row_is_included_by_frame_exclusion(clause, partition_rows, row_index, candidate)
                    .map(|included| included.then_some(candidate))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row_index in included_indexes.into_iter().flatten() {
            advance_window_aggregate(ctx, &mut state, &mut partition_rows[row_index].row, aggref)?;
        }
        values.push(state.finalize());
    }
    Ok(values)
}

fn evaluate_peer_prefix_aggregate_window(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    aggref: &crate::include::nodes::primnodes::Aggref,
    partition_rows: &mut [PreparedWindowRow],
    func: AggFunc,
) -> Result<Option<Vec<Value>>, ExecError> {
    let frame = &clause.spec.frame;
    if !matches!(frame.mode, WindowFrameMode::Range | WindowFrameMode::Groups)
        || !matches!(frame.start_bound, WindowFrameBound::UnboundedPreceding)
        || !matches!(frame.end_bound, WindowFrameBound::CurrentRow)
        || frame.exclusion != WindowFrameExclusion::NoOthers
        || clause.spec.order_by.is_empty()
    {
        return Ok(None);
    }

    if func == AggFunc::Count
        && !aggref.aggdistinct
        && aggref.args.is_empty()
        && aggref.aggfilter.is_none()
    {
        let mut values = Vec::with_capacity(partition_rows.len());
        let mut group_start = 0usize;
        while group_start < partition_rows.len() {
            let group_end =
                peer_group_end_for_index(partition_rows, &clause.spec.order_by, group_start)?;
            let value = Value::Int64(group_end as i64);
            values.extend(std::iter::repeat_n(value, group_end - group_start));
            group_start = group_end;
        }
        return Ok(Some(values));
    }

    let mut state = AccumState::new(func, aggref.aggdistinct, aggref.aggtype);
    let mut values = Vec::with_capacity(partition_rows.len());
    let mut group_start = 0usize;
    while group_start < partition_rows.len() {
        let group_end =
            peer_group_end_for_index(partition_rows, &clause.spec.order_by, group_start)?;
        for row in partition_rows.iter_mut().take(group_end).skip(group_start) {
            advance_window_aggregate(ctx, &mut state, &mut row.row, aggref)?;
        }
        let value = state.finalize();
        values.extend(std::iter::repeat_n(value, group_end - group_start));
        group_start = group_end;
    }

    Ok(Some(values))
}

fn frame_uses_prefix_accumulation(
    clause: &WindowClause,
    partition_rows: &mut [PreparedWindowRow],
    ctx: &mut ExecutorContext,
    _aggref: &crate::include::nodes::primnodes::Aggref,
) -> Result<bool, ExecError> {
    let frame = &clause.spec.frame;
    if frame.exclusion != WindowFrameExclusion::NoOthers {
        return Ok(false);
    }
    if !matches!(frame.start_bound, WindowFrameBound::UnboundedPreceding) {
        return Ok(false);
    }
    if partition_rows.is_empty() {
        return Ok(true);
    }

    match (&frame.mode, &frame.end_bound) {
        (WindowFrameMode::Rows, WindowFrameBound::CurrentRow)
        | (WindowFrameMode::Groups, WindowFrameBound::CurrentRow)
        | (WindowFrameMode::Range, WindowFrameBound::CurrentRow) => {}
        (WindowFrameMode::Rows, WindowFrameBound::OffsetFollowing(_))
        | (WindowFrameMode::Groups, WindowFrameBound::OffsetFollowing(_))
        | (WindowFrameMode::Range, WindowFrameBound::OffsetFollowing(_)) => {}
        _ => return Ok(false),
    }

    let mut previous_end = 0usize;
    for row_index in 0..partition_rows.len() {
        let (frame_start, frame_end) =
            evaluate_window_frame(ctx, clause, partition_rows, row_index)?;
        if frame_start != 0 || frame_end < previous_end {
            return Ok(false);
        }
        previous_end = frame_end;
    }

    Ok(true)
}

pub(crate) fn execute_window_clause(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    rows: Vec<MaterializedRow>,
) -> Result<Vec<MaterializedRow>, ExecError> {
    let mut prepared = prepare_input_rows(ctx, clause, rows)?;
    let mut output_rows = Vec::with_capacity(prepared.len());
    let mut partition_start = 0usize;
    while partition_start < prepared.len() {
        let mut partition_end = partition_start + 1;
        while partition_end < prepared.len()
            && same_partition(&prepared[partition_end - 1], &prepared[partition_end])
        {
            partition_end += 1;
        }

        let partition = &mut prepared[partition_start..partition_end];
        let mut function_values = Vec::with_capacity(clause.functions.len());
        for func in &clause.functions {
            function_values.push(match &func.kind {
                WindowFuncKind::Builtin(_) => {
                    evaluate_builtin_window(ctx, clause, func, partition)?
                }
                WindowFuncKind::Aggregate(aggref) => evaluate_aggregate_window(
                    ctx,
                    clause,
                    aggref,
                    partition,
                    !clause.spec.order_by.is_empty(),
                )?,
            });
        }

        for (row_index, prepared_row) in partition.iter().enumerate() {
            let mut values = prepared_row.row.slot.tts_values.clone();
            for results in &function_values {
                values.push(results[row_index].clone());
            }
            output_rows.push(MaterializedRow::new(
                TupleSlot::virtual_row(values),
                prepared_row.row.system_bindings.clone(),
            ));
        }

        partition_start = partition_end;
    }
    Ok(output_rows)
}
