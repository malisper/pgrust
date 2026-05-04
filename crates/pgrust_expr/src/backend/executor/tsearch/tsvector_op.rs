use std::cmp::Ordering;

use super::super::ExecError;
use crate::compat::include::catalog::{INT2_TYPE_OID, TEXT_TYPE_OID};
use crate::compat::include::nodes::datum::{ArrayValue, Value};
use crate::compat::include::nodes::tsearch::{TsLexeme, TsPosition, TsVector, TsWeight};

pub fn compare_tsvector(left: &TsVector, right: &TsVector) -> Ordering {
    left.render().cmp(&right.render())
}

pub fn concat_tsvector(left: &TsVector, right: &TsVector) -> TsVector {
    let max_left_pos = left
        .lexemes
        .iter()
        .flat_map(|lexeme| lexeme.positions.iter().map(|position| position.position))
        .max()
        .unwrap_or(0);
    let mut merged = left.lexemes.clone();
    merged.extend(right.lexemes.iter().map(|lexeme| {
        TsLexeme {
            text: lexeme.text.clone(),
            positions: lexeme
                .positions
                .iter()
                .map(|position| TsPosition {
                    position: position.position.saturating_add(max_left_pos),
                    weight: position.weight,
                })
                .collect(),
        }
    }));
    TsVector::new(merged)
}

pub fn strip_tsvector(vector: &TsVector) -> TsVector {
    TsVector::new(
        vector
            .lexemes
            .iter()
            .map(|lexeme| TsLexeme {
                text: lexeme.text.clone(),
                positions: Vec::new(),
            })
            .collect(),
    )
}

pub fn delete_tsvector_lexemes(vector: &TsVector, lexemes: &[String]) -> TsVector {
    let delete = lexemes
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    TsVector::new(
        vector
            .lexemes
            .iter()
            .filter(|lexeme| !delete.contains(lexeme.text.as_str()))
            .cloned()
            .collect(),
    )
}

pub fn tsvector_to_array(vector: &TsVector) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            vector
                .lexemes
                .iter()
                .map(|lexeme| Value::Text(lexeme.text.clone()))
                .collect(),
        )
        .with_element_type_oid(TEXT_TYPE_OID),
    )
}

pub fn array_to_tsvector(value: &Value) -> Result<TsVector, ExecError> {
    let lexemes = text_array_items(value, "array_to_tsvector")?;
    let mut out = Vec::with_capacity(lexemes.len());
    for item in lexemes {
        match item {
            None => {
                return Err(ExecError::DetailedError {
                    message: "lexeme array may not contain nulls".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22004",
                });
            }
            Some(text) if text.is_empty() => {
                return Err(ExecError::DetailedError {
                    message: "lexeme array may not contain empty strings".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
            Some(text) => out.push(TsLexeme {
                text: text.into(),
                positions: Vec::new(),
            }),
        }
    }
    Ok(TsVector::new(out))
}

pub fn setweight_tsvector(
    vector: &TsVector,
    weight: TsWeight,
    filter: Option<&Value>,
) -> Result<TsVector, ExecError> {
    let filter = filter
        .map(|value| text_array_items(value, "setweight"))
        .transpose()?
        .map(|items| {
            items
                .into_iter()
                .flatten()
                .collect::<std::collections::BTreeSet<_>>()
        });
    Ok(TsVector::new(
        vector
            .lexemes
            .iter()
            .map(|lexeme| {
                let selected = filter
                    .as_ref()
                    .is_none_or(|items| items.contains(lexeme.text.as_str()));
                TsLexeme {
                    text: lexeme.text.clone(),
                    positions: if selected {
                        lexeme
                            .positions
                            .iter()
                            .map(|position| TsPosition {
                                position: position.position,
                                weight: (weight != TsWeight::D).then_some(weight),
                            })
                            .collect()
                    } else {
                        lexeme.positions.clone()
                    },
                }
            })
            .collect(),
    ))
}

pub fn filter_tsvector(vector: &TsVector, weights: &Value) -> Result<TsVector, ExecError> {
    let keep = weight_array_items(weights, "ts_filter")?;
    Ok(TsVector::new(
        vector
            .lexemes
            .iter()
            .filter_map(|lexeme| {
                let positions = lexeme
                    .positions
                    .iter()
                    .copied()
                    .filter(|position| keep.contains(&position.weight.unwrap_or(TsWeight::D)))
                    .collect::<Vec<_>>();
                (!positions.is_empty()).then(|| TsLexeme {
                    text: lexeme.text.clone(),
                    positions,
                })
            })
            .collect(),
    ))
}

pub fn unnest_tsvector(vector: &TsVector) -> Vec<Value> {
    vector
        .lexemes
        .iter()
        .map(|lexeme| {
            let positions = if lexeme.positions.is_empty() {
                Value::Null
            } else {
                Value::PgArray(
                    ArrayValue::from_1d(
                        lexeme
                            .positions
                            .iter()
                            .map(|position| Value::Int16(position.position as i16))
                            .collect(),
                    )
                    .with_element_type_oid(INT2_TYPE_OID),
                )
            };
            let weights = if lexeme.positions.is_empty() {
                Value::Null
            } else {
                Value::PgArray(
                    ArrayValue::from_1d(
                        lexeme
                            .positions
                            .iter()
                            .map(|position| {
                                Value::Text(
                                    position
                                        .weight
                                        .unwrap_or(TsWeight::D)
                                        .as_char()
                                        .to_string()
                                        .into(),
                                )
                            })
                            .collect(),
                    )
                    .with_element_type_oid(TEXT_TYPE_OID),
                )
            };
            Value::Record(
                crate::compat::include::nodes::datum::RecordValue::anonymous(vec![
                    ("lexeme".into(), Value::Text(lexeme.text.clone())),
                    ("positions".into(), positions),
                    ("weights".into(), weights),
                ]),
            )
        })
        .collect()
}

pub fn parse_ts_weight(value: &Value, op: &'static str) -> Result<TsWeight, ExecError> {
    let text = match value {
        Value::InternalChar(byte) => {
            crate::compat::backend::executor::render_internal_char_text(*byte)
        }
        other => other.as_text().unwrap_or_default().to_string(),
    };
    let Some(ch) = text.chars().next() else {
        return Err(ExecError::DetailedError {
            message: "unrecognized weight".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    };
    TsWeight::from_char(ch).ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: value.clone(),
        right: Value::Null,
    })
}

pub fn text_array_items(value: &Value, op: &'static str) -> Result<Vec<Option<String>>, ExecError> {
    let values = match value {
        Value::Null => return Ok(Vec::new()),
        Value::Array(values) => values.clone(),
        Value::PgArray(array) => array.to_nested_values(),
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    values
        .into_iter()
        .map(|item| {
            if matches!(item, Value::Null) {
                Ok(None)
            } else {
                item.as_text()
                    .map(|text| Some(text.to_string()))
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: item,
                        right: Value::Null,
                    })
            }
        })
        .collect()
}

fn weight_array_items(value: &Value, op: &'static str) -> Result<Vec<TsWeight>, ExecError> {
    let values = match value {
        Value::Array(values) => values.clone(),
        Value::PgArray(array) => array.to_nested_values(),
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    let mut out = Vec::new();
    for item in values {
        if matches!(item, Value::Null) {
            return Err(ExecError::DetailedError {
                message: "weight array may not contain nulls".into(),
                detail: None,
                hint: None,
                sqlstate: "22004",
            });
        }
        out.push(parse_ts_weight(&item, op)?);
    }
    Ok(out)
}
