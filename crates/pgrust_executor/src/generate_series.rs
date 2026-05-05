use std::cmp::Ordering;

use pgrust_expr::utils::time::datetime::{
    days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use pgrust_nodes::SqlTypeKind;
use pgrust_nodes::datetime::{
    TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimestampADT, TimestampTzADT, USECS_PER_DAY,
};
use pgrust_nodes::datum::{IntervalValue, NumericValue, Value};

pub const MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS: usize = 10_000;

#[derive(Debug, Clone, PartialEq)]
pub enum GenerateSeriesError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    ZeroStep,
    InfiniteStep,
    InvalidArg(&'static str, &'static str),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GenerateSeriesState {
    Numeric {
        current: NumericValue,
        stop: NumericValue,
        step: NumericValue,
        step_cmp: Ordering,
        dscale: u32,
    },
    Integral {
        current: i64,
        stop: i64,
        step: i64,
        output_kind: SqlTypeKind,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampGenerateSeriesState {
    current: i64,
    end: i64,
    step: IntervalValue,
    sign: i32,
    output_kind: SqlTypeKind,
    emitted: usize,
    finished: bool,
}

impl GenerateSeriesState {
    pub fn numeric(start: Value, stop: Value, step: Value) -> Result<Self, GenerateSeriesError> {
        let start = generate_series_numeric_arg(start, "generate_series start")?;
        let stop = generate_series_numeric_arg(stop, "generate_series stop")?;
        let step = generate_series_numeric_arg(step, "generate_series step")?;
        validate_generate_series_numeric_arg(&start, "start")?;
        validate_generate_series_numeric_arg(&stop, "stop")?;
        validate_generate_series_numeric_arg(&step, "step size")?;
        let dscale = [start.dscale(), stop.dscale(), step.dscale()]
            .into_iter()
            .max()
            .unwrap_or(0);
        let step_cmp = step.cmp(&NumericValue::zero());
        if step_cmp == Ordering::Equal {
            return Err(GenerateSeriesError::ZeroStep);
        }
        Ok(GenerateSeriesState::Numeric {
            current: start,
            stop,
            step,
            step_cmp,
            dscale,
        })
    }

    pub fn integral(
        start: Value,
        stop: Value,
        step: Value,
        output_kind: SqlTypeKind,
    ) -> Result<Self, GenerateSeriesError> {
        let current = generate_series_i64_arg(start, "generate_series start")?;
        let stop = generate_series_i64_arg(stop, "generate_series stop")?;
        let step = generate_series_i64_arg(step, "generate_series step")?;
        if step == 0 {
            return Err(GenerateSeriesError::ZeroStep);
        }
        Ok(GenerateSeriesState::Integral {
            current,
            stop,
            step,
            output_kind,
        })
    }

    pub fn next_value(&mut self) -> Option<Value> {
        match self {
            GenerateSeriesState::Numeric {
                current,
                stop,
                step,
                step_cmp,
                dscale,
            } => {
                let done = match step_cmp {
                    Ordering::Greater => current.cmp(stop) == Ordering::Greater,
                    Ordering::Less => current.cmp(stop) == Ordering::Less,
                    Ordering::Equal => unreachable!(),
                };
                if done {
                    return None;
                }
                let value = current.clone().with_dscale(*dscale);
                *current = current.add(step).with_dscale(*dscale);
                Some(Value::Numeric(value))
            }
            GenerateSeriesState::Integral {
                current,
                stop,
                step,
                output_kind,
            } => {
                let done = if *step > 0 {
                    *current > *stop
                } else {
                    *current < *stop
                };
                if done {
                    return None;
                }
                let value = *current;
                *current += *step;
                Some(match output_kind {
                    SqlTypeKind::Int8 => Value::Int64(value),
                    _ => Value::Int32(value as i32),
                })
            }
        }
    }
}

impl TimestampGenerateSeriesState {
    pub fn new(
        start: Value,
        stop: Value,
        step: Value,
        output_kind: SqlTypeKind,
    ) -> Result<Self, GenerateSeriesError> {
        let (current, end) = match (start, stop, output_kind) {
            (Value::Timestamp(start), Value::Timestamp(stop), SqlTypeKind::Timestamp) => {
                (start.0, stop.0)
            }
            (Value::TimestampTz(start), Value::TimestampTz(stop), SqlTypeKind::TimestampTz) => {
                (start.0, stop.0)
            }
            (start, stop, _) => {
                return Err(GenerateSeriesError::TypeMismatch {
                    op: "generate_series",
                    left: start,
                    right: stop,
                });
            }
        };
        let Value::Interval(step) = step else {
            return Err(GenerateSeriesError::TypeMismatch {
                op: "generate_series step",
                left: step,
                right: Value::Interval(IntervalValue::zero()),
            });
        };
        if !step.is_finite() {
            return Err(GenerateSeriesError::InfiniteStep);
        }
        let sign = interval_sign(step);
        if sign == 0 {
            return Err(GenerateSeriesError::ZeroStep);
        }
        Ok(Self {
            current,
            end,
            step,
            sign,
            output_kind,
            emitted: 0,
            finished: false,
        })
    }

    pub fn next_value(&mut self) -> Result<Option<Value>, GenerateSeriesError> {
        if self.finished {
            return Ok(None);
        }
        let done = if self.sign > 0 {
            self.current > self.end
        } else {
            self.current < self.end
        };
        if done {
            return Ok(None);
        }
        let value = match self.output_kind {
            SqlTypeKind::TimestampTz => Value::TimestampTz(TimestampTzADT(self.current)),
            _ => Value::Timestamp(TimestampADT(self.current)),
        };
        self.emitted += 1;
        if matches!(self.end, TIMESTAMP_NOEND | TIMESTAMP_NOBEGIN)
            && self.emitted >= MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS
        {
            self.finished = true;
            return Ok(Some(value));
        }
        let Some(next) = timestamp_add_interval(self.current, self.step) else {
            self.finished = true;
            return Ok(Some(value));
        };
        if next == self.current {
            return Err(GenerateSeriesError::ZeroStep);
        }
        self.current = next;
        Ok(Some(value))
    }
}

fn timestamp_add_interval(base: i64, step: IntervalValue) -> Option<i64> {
    if !step.is_finite() || base == i64::MIN || base == i64::MAX {
        return None;
    }
    let (days, time) = timestamp_parts_from_usecs(base);
    let (year, month, day) = ymd_from_days(days);
    let month_index = i64::from(year) * 12 + i64::from(month - 1) + i64::from(step.months);
    let new_year = month_index.div_euclid(12) as i32;
    let new_month = month_index.rem_euclid(12) as u32 + 1;
    let new_day = day.min(days_in_month(new_year, new_month));
    let new_days = days_from_ymd(new_year, new_month, new_day)?;
    i64::from(new_days)
        .checked_mul(USECS_PER_DAY)?
        .checked_add(time)?
        .checked_add(i64::from(step.days).checked_mul(USECS_PER_DAY)?)?
        .checked_add(step.time_micros)
}

fn interval_sign(step: IntervalValue) -> i32 {
    let key = step.cmp_key();
    if key > 0 {
        1
    } else if key < 0 {
        -1
    } else {
        0
    }
}

fn generate_series_numeric_arg(
    value: Value,
    label: &'static str,
) -> Result<NumericValue, GenerateSeriesError> {
    match value {
        Value::Numeric(n) => Ok(n),
        Value::Int32(i) => Ok(NumericValue::from_i64(i64::from(i))),
        Value::Int64(i) => Ok(NumericValue::from_i64(i)),
        other => Err(GenerateSeriesError::TypeMismatch {
            op: label,
            left: other,
            right: Value::Null,
        }),
    }
}

fn validate_generate_series_numeric_arg(
    value: &NumericValue,
    arg: &'static str,
) -> Result<(), GenerateSeriesError> {
    match value {
        NumericValue::NaN => Err(GenerateSeriesError::InvalidArg(arg, "NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            Err(GenerateSeriesError::InvalidArg(arg, "infinity"))
        }
        NumericValue::Finite { .. } => Ok(()),
    }
}

fn generate_series_i64_arg(value: Value, label: &'static str) -> Result<i64, GenerateSeriesError> {
    match value {
        Value::Int32(v) => Ok(i64::from(v)),
        Value::Int64(v) => Ok(v),
        other => Err(GenerateSeriesError::TypeMismatch {
            op: label,
            left: other,
            right: Value::Null,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(mut state: GenerateSeriesState) -> Vec<Value> {
        let mut values = Vec::new();
        while let Some(value) = state.next_value() {
            values.push(value);
        }
        values
    }

    #[test]
    fn integral_series_counts_up_and_down() {
        let up = GenerateSeriesState::integral(
            Value::Int32(1),
            Value::Int32(5),
            Value::Int32(2),
            SqlTypeKind::Int4,
        )
        .unwrap();
        assert_eq!(
            collect(up),
            vec![Value::Int32(1), Value::Int32(3), Value::Int32(5)]
        );

        let down = GenerateSeriesState::integral(
            Value::Int64(5),
            Value::Int64(1),
            Value::Int64(-2),
            SqlTypeKind::Int8,
        )
        .unwrap();
        assert_eq!(
            collect(down),
            vec![Value::Int64(5), Value::Int64(3), Value::Int64(1)]
        );
    }

    #[test]
    fn zero_step_is_rejected() {
        assert_eq!(
            GenerateSeriesState::integral(
                Value::Int32(1),
                Value::Int32(5),
                Value::Int32(0),
                SqlTypeKind::Int4,
            )
            .unwrap_err(),
            GenerateSeriesError::ZeroStep
        );
    }

    #[test]
    fn numeric_series_preserves_max_scale() {
        let state = GenerateSeriesState::numeric(
            Value::Numeric(NumericValue::from_i64(1).with_dscale(1)),
            Value::Numeric(NumericValue::from_i64(3).with_dscale(0)),
            Value::Numeric(NumericValue::from_i64(1).with_dscale(2)),
        )
        .unwrap();

        assert_eq!(
            collect(state),
            vec![
                Value::Numeric(NumericValue::from_i64(1).with_dscale(2)),
                Value::Numeric(NumericValue::from_i64(2).with_dscale(2)),
                Value::Numeric(NumericValue::from_i64(3).with_dscale(2)),
            ]
        );
    }

    #[test]
    fn invalid_numeric_args_are_rejected() {
        assert_eq!(
            GenerateSeriesState::numeric(
                Value::Numeric(NumericValue::NaN),
                Value::Numeric(NumericValue::from_i64(3)),
                Value::Numeric(NumericValue::from_i64(1)),
            )
            .unwrap_err(),
            GenerateSeriesError::InvalidArg("start", "NaN")
        );
    }

    #[test]
    fn timestamp_series_steps_by_interval() {
        let mut state = TimestampGenerateSeriesState::new(
            Value::Timestamp(TimestampADT(0)),
            Value::Timestamp(TimestampADT(2 * USECS_PER_DAY)),
            Value::Interval(IntervalValue {
                months: 0,
                days: 1,
                time_micros: 0,
            }),
            SqlTypeKind::Timestamp,
        )
        .unwrap();

        assert_eq!(
            state.next_value().unwrap(),
            Some(Value::Timestamp(TimestampADT(0)))
        );
        assert_eq!(
            state.next_value().unwrap(),
            Some(Value::Timestamp(TimestampADT(USECS_PER_DAY)))
        );
        assert_eq!(
            state.next_value().unwrap(),
            Some(Value::Timestamp(TimestampADT(2 * USECS_PER_DAY)))
        );
        assert_eq!(state.next_value().unwrap(), None);
    }

    #[test]
    fn timestamp_series_caps_unbounded_infinity() {
        let mut state = TimestampGenerateSeriesState::new(
            Value::Timestamp(TimestampADT(0)),
            Value::Timestamp(TimestampADT(TIMESTAMP_NOEND)),
            Value::Interval(IntervalValue {
                months: 1,
                days: 0,
                time_micros: 0,
            }),
            SqlTypeKind::Timestamp,
        )
        .unwrap();

        let mut emitted = 0usize;
        while state.next_value().unwrap().is_some() {
            emitted += 1;
            assert!(
                emitted <= MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS,
                "unbounded timestamp series did not stop at cap"
            );
        }
        assert_eq!(emitted, MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS);
    }

    #[test]
    fn timestamp_series_rejects_zero_and_infinite_steps() {
        assert_eq!(
            TimestampGenerateSeriesState::new(
                Value::Timestamp(TimestampADT(0)),
                Value::Timestamp(TimestampADT(1)),
                Value::Interval(IntervalValue::zero()),
                SqlTypeKind::Timestamp,
            )
            .unwrap_err(),
            GenerateSeriesError::ZeroStep
        );
        assert_eq!(
            TimestampGenerateSeriesState::new(
                Value::Timestamp(TimestampADT(0)),
                Value::Timestamp(TimestampADT(1)),
                Value::Interval(IntervalValue::infinity()),
                SqlTypeKind::Timestamp,
            )
            .unwrap_err(),
            GenerateSeriesError::InfiniteStep
        );
    }
}
