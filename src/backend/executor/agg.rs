use super::{compare_order_values, parse_numeric_text};
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::{NumericValue, Value};
use crate::include::nodes::plannodes::AggFunc;
use crate::pgrust::session::ByteaOutputFormat;
use super::render_bit_text;

use std::cmp::Ordering;
use std::collections::HashSet;

use super::jsonb::{JsonbValue, encode_jsonb, jsonb_from_value, render_jsonb_bytes};

#[derive(Debug, Clone)]
pub(crate) enum NumericAccum {
    Int(i64),
    Float(f64),
    Numeric(NumericValue),
}

#[derive(Debug, Clone)]
pub(crate) enum AccumState {
    Count {
        count: i64,
    },
    CountDistinct {
        seen: HashSet<Value>,
    },
    Sum {
        sum: Option<NumericAccum>,
        result_type: SqlType,
    },
    Avg {
        sum: Option<NumericAccum>,
        count: i64,
        result_type: SqlType,
    },
    JsonAgg {
        values: Vec<Value>,
        jsonb: bool,
    },
    JsonObjectAgg {
        pairs: Vec<(Value, Value)>,
        jsonb: bool,
    },
    Min {
        min: Option<Value>,
    },
    Max {
        max: Option<Value>,
    },
}

impl AccumState {
    pub(crate) fn new(func: AggFunc, distinct: bool, sql_type: SqlType) -> Self {
        match (func, distinct) {
            (AggFunc::Count, true) => AccumState::CountDistinct {
                seen: HashSet::new(),
            },
            (AggFunc::Count, false) => AccumState::Count { count: 0 },
            (AggFunc::Sum, _) => AccumState::Sum {
                sum: None,
                result_type: sql_type,
            },
            (AggFunc::Avg, _) => AccumState::Avg {
                sum: None,
                count: 0,
                result_type: sql_type,
            },
            (AggFunc::JsonAgg, _) => AccumState::JsonAgg {
                values: Vec::new(),
                jsonb: false,
            },
            (AggFunc::JsonbAgg, _) => AccumState::JsonAgg {
                values: Vec::new(),
                jsonb: true,
            },
            (AggFunc::JsonObjectAgg, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: false,
            },
            (AggFunc::JsonbObjectAgg, _) => AccumState::JsonObjectAgg {
                pairs: Vec::new(),
                jsonb: true,
            },
            (AggFunc::Min, _) => AccumState::Min { min: None },
            (AggFunc::Max, _) => AccumState::Max { max: None },
        }
    }

    pub(crate) fn transition_fn(
        func: AggFunc,
        arg_count: usize,
        distinct: bool,
    ) -> fn(&mut AccumState, &[Value]) {
        match (func, arg_count, distinct) {
            (AggFunc::Count, _, true) => |state, values| {
                if let AccumState::CountDistinct { seen } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        seen.insert(value.to_owned_value());
                    }
                }
            },
            (AggFunc::Count, 0, false) => |state, _values| {
                if let AccumState::Count { count } = state {
                    *count += 1;
                }
            },
            (AggFunc::Count, _, false) => |state, values| {
                if let AccumState::Count { count } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *count += 1;
                    }
                }
            },
            (AggFunc::Sum, _, _) => |state, values| {
                if let AccumState::Sum { sum, result_type } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    *sum = accumulate_value(sum.take(), *result_type, value);
                }
            },
            (AggFunc::Avg, _, _) => |state, values| {
                if let AccumState::Avg {
                    sum,
                    count,
                    result_type,
                } = state
                {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *sum = accumulate_value(sum.take(), *result_type, value);
                        *count += 1;
                    }
                }
            },
            (AggFunc::JsonAgg | AggFunc::JsonbAgg, _, _) => |state, arg_values| {
                if let AccumState::JsonAgg { values, .. } = state {
                    let value = arg_values.first().unwrap_or(&Value::Null);
                    values.push(value.to_owned_value());
                }
            },
            (AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg, _, _) => |state, values| {
                if let AccumState::JsonObjectAgg { pairs, .. } = state {
                    let key = values.first().unwrap_or(&Value::Null);
                    let value = values.get(1).unwrap_or(&Value::Null);
                    pairs.push((key.to_owned_value(), value.to_owned_value()));
                }
            },
            (AggFunc::Min, _, _) => |state, values| {
                if let AccumState::Min { min } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *min = Some(match min.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false)
                                    == Ordering::Less
                                {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
            },
            (AggFunc::Max, _, _) => |state, values| {
                if let AccumState::Max { max } = state {
                    let value = values.first().unwrap_or(&Value::Null);
                    if !matches!(value, Value::Null) {
                        *max = Some(match max.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false)
                                    == Ordering::Greater
                                {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
            },
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AccumState::Count { count } => Value::Int64(*count),
            AccumState::CountDistinct { seen } => Value::Int64(seen.len() as i64),
            AccumState::Sum { sum, result_type } => match sum {
                Some(NumericAccum::Int(v)) => Value::Int64(*v),
                Some(NumericAccum::Float(v)) => Value::Float64(*v),
                Some(NumericAccum::Numeric(v)) => {
                    Value::Numeric(format_numeric_result(v.clone(), *result_type))
                }
                None => Value::Null,
            },
            AccumState::Avg {
                sum,
                count,
                result_type,
            } => {
                if *count == 0 {
                    Value::Null
                } else {
                    match sum {
                        Some(NumericAccum::Int(v)) => {
                            if matches!(result_type.kind, SqlTypeKind::Numeric) {
                                let avg = NumericValue::from_i64(*v)
                                    .div(&NumericValue::from_i64(*count), 16)
                                    .unwrap_or_else(|| NumericValue::from_i64(*v / *count));
                                Value::Numeric(format_numeric_result(avg, *result_type))
                            } else {
                                Value::Int64(*v / *count)
                            }
                        }
                        Some(NumericAccum::Float(v)) => Value::Float64(*v / *count as f64),
                        Some(NumericAccum::Numeric(v)) => {
                            let avg = v
                                .div(&NumericValue::from_i64(*count), 16)
                                .unwrap_or_else(|| v.clone());
                            Value::Numeric(format_numeric_result(avg, *result_type))
                        }
                        None => Value::Null,
                    }
                }
            }
            AccumState::JsonAgg { values, jsonb } => {
                if *jsonb {
                    let mut items = Vec::with_capacity(values.len());
                    for value in values {
                        items.push(jsonb_from_value(value).unwrap_or(JsonbValue::Null));
                    }
                    Value::Jsonb(encode_jsonb(&JsonbValue::Array(items)))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_array(values),
                    ))
                }
            }
            AccumState::JsonObjectAgg { pairs, jsonb } => {
                if *jsonb {
                    let built = JsonbValue::Object(
                        pairs
                            .iter()
                            .map(|(k, v)| {
                                (
                                    json_object_agg_key(k),
                                    jsonb_from_value(v).unwrap_or(JsonbValue::Null),
                                )
                            })
                            .collect(),
                    );
                    Value::Jsonb(encode_jsonb(&built))
                } else {
                    Value::Json(crate::pgrust::compact_string::CompactString::from_owned(
                        render_json_object(pairs),
                    ))
                }
            }
            AccumState::Min { min } => min.clone().unwrap_or(Value::Null),
            AccumState::Max { max } => max.clone().unwrap_or(Value::Null),
        }
    }
}

fn render_json_array(values: &[Value]) -> String {
    let mut out = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&value_to_json_text(value));
    }
    out.push(']');
    out
}

fn render_json_object(pairs: &[(Value, Value)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let key_text = json_object_agg_key(key);
        out.push_str(&serde_json::to_string(&key_text).unwrap());
        out.push(':');
        out.push_str(&value_to_json_text(value));
    }
    out.push('}');
    out
}

fn json_object_agg_key(key: &Value) -> String {
    match key {
        Value::Null => "null".to_string(),
        Value::Text(_) | Value::TextRef(_, _) => key.as_text().unwrap().to_string(),
        Value::Bit(v) => render_bit_text(v),
        Value::Bytea(v) => format_bytea_text(v, ByteaOutputFormat::Hex),
        Value::InternalChar(v) => crate::backend::executor::render_internal_char_text(*v),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => render_jsonb_bytes(v).unwrap_or_else(|_| "null".into()),
        Value::Numeric(v) => v.render(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::JsonPath(v) => v.to_string(),
        Value::Array(_) => value_to_json_text(key),
    }
}

fn value_to_json_text(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::Bit(v) => serde_json::to_string(&render_bit_text(v)).unwrap(),
        Value::JsonPath(v) => serde_json::to_string(v.as_str()).unwrap(),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => render_jsonb_bytes(v).unwrap_or_else(|_| "null".into()),
        Value::Text(_) | Value::TextRef(_, _) => {
            serde_json::to_string(value.as_text().unwrap()).unwrap()
        }
        Value::Bytea(v) => serde_json::to_string(&format_bytea_text(v, ByteaOutputFormat::Hex)).unwrap(),
        Value::InternalChar(v) => serde_json::to_string(
            &crate::backend::executor::render_internal_char_text(*v),
        )
        .unwrap(),
        Value::Array(items) => render_json_array(items),
    }
}

fn accumulate_value(sum: Option<NumericAccum>, result_type: SqlType, value: &Value) -> Option<NumericAccum> {
    match value {
        Value::Null => sum,
        Value::Int16(v) => Some(accumulate_integral(sum, result_type, *v as i64)),
        Value::Int32(v) => Some(accumulate_integral(sum, result_type, *v as i64)),
        Value::Int64(v) => Some(accumulate_integral(sum, result_type, *v)),
        Value::Float64(v) => Some(match sum {
            Some(NumericAccum::Numeric(cur)) => {
                let rhs = parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero);
                NumericAccum::Numeric(cur.add(&rhs))
            }
            Some(NumericAccum::Int(cur)) => NumericAccum::Float(cur as f64 + *v),
            Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + *v),
            None => {
                if matches!(result_type.kind, SqlTypeKind::Numeric) {
                    NumericAccum::Numeric(
                        parse_numeric_text(&v.to_string()).unwrap_or_else(NumericValue::zero),
                    )
                } else {
                    NumericAccum::Float(*v)
                }
            }
        }),
        Value::Numeric(v) => {
            let parsed = v.clone();
            Some(match sum {
                Some(NumericAccum::Numeric(cur)) => NumericAccum::Numeric(cur.add(&parsed)),
                Some(NumericAccum::Int(cur)) => {
                    NumericAccum::Numeric(NumericValue::from_i64(cur).add(&parsed))
                }
                Some(NumericAccum::Float(cur)) => {
                    let left =
                        parse_numeric_text(&cur.to_string()).unwrap_or_else(NumericValue::zero);
                    NumericAccum::Numeric(left.add(&parsed))
                }
                None => NumericAccum::Numeric(parsed),
            })
        }
        _ => sum,
    }
}

fn accumulate_integral(sum: Option<NumericAccum>, result_type: SqlType, value: i64) -> NumericAccum {
    match sum {
        Some(NumericAccum::Numeric(cur)) => {
            NumericAccum::Numeric(cur.add(&NumericValue::from_i64(value)))
        }
        Some(NumericAccum::Int(cur)) => NumericAccum::Int(cur + value),
        Some(NumericAccum::Float(cur)) => NumericAccum::Float(cur + value as f64),
        None => {
            if matches!(result_type.kind, SqlTypeKind::Numeric) {
                NumericAccum::Numeric(NumericValue::from_i64(value))
            } else {
                NumericAccum::Int(value)
            }
        }
    }
}

fn format_numeric_result(value: NumericValue, sql_type: SqlType) -> NumericValue {
    if let Some((_, scale)) = sql_type.numeric_precision_scale() {
        value.round_to_scale(scale as u32).unwrap_or(value)
    } else {
        value
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AggGroup {
    pub(crate) key_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
}
