use pgrust_nodes::datum::{ArrayDimension, ArrayValue, Value};

use super::ExecError;

pub use super::expr_ops::parse_numeric_text;
pub use super::value_io::format_array_text;

pub fn eval_expr(
    _expr: &pgrust_nodes::primnodes::Expr,
    _slot: &mut super::TupleSlot,
    _ctx: &mut super::ExecutorContext,
) -> Result<Value, ExecError> {
    Err(ExecError::DetailedError {
        message: "expression evaluation requires root executor".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

pub fn append_array_value(
    array: &Value,
    element: &Value,
    prepend: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        let mut result = ArrayValue::from_1d(vec![element.to_owned_value()]);
        if let Some(element_type_oid) = array_element_type_oid_from_value(element) {
            result = result.with_element_type_oid(element_type_oid);
        }
        return Ok(Value::PgArray(result));
    };
    let element_type_oid = array.element_type_oid;
    if array.ndim() == 0 {
        let mut result = ArrayValue::from_1d(vec![element.clone()]);
        if let Some(element_type_oid) = element_type_oid {
            result = result.with_element_type_oid(element_type_oid);
        }
        return Ok(Value::PgArray(result));
    }

    let mut result = if let Some(element_array) = normalize_array_value(element) {
        concatenate_rank_mismatched_arrays(array, element_array, prepend)?
    } else {
        if array.ndim() != 1 {
            return incompatible_array_concat_error();
        }
        let mut elements = array.elements.clone();
        if prepend {
            elements.insert(0, element.clone());
        } else {
            elements.push(element.clone());
        }
        let mut dimensions = array.dimensions.clone();
        dimensions[0].length += 1;
        ArrayValue::from_dimensions(dimensions, elements)
    };
    if let Some(element_type_oid) = element_type_oid {
        result = result.with_element_type_oid(element_type_oid);
    }
    Ok(Value::PgArray(result))
}

pub fn concatenate_arrays(left: ArrayValue, right: ArrayValue) -> Result<ArrayValue, ExecError> {
    if left.ndim() == 0 {
        return Ok(right);
    }
    if right.ndim() == 0 {
        return Ok(left);
    }
    if left.ndim() == right.ndim() {
        if left.dimensions[1..] != right.dimensions[1..] {
            return incompatible_array_concat_error();
        }
        let mut dimensions = left.dimensions.clone();
        dimensions[0].length += right.dimensions[0].length;
        let mut elements = left.elements;
        elements.extend(right.elements);
        return Ok(ArrayValue {
            element_type_oid: left.element_type_oid.or(right.element_type_oid),
            dimensions,
            elements,
        });
    }
    if left.ndim() + 1 == right.ndim() {
        return concatenate_rank_mismatched_arrays(right, left, true);
    }
    if right.ndim() + 1 == left.ndim() {
        return concatenate_rank_mismatched_arrays(left, right, false);
    }
    incompatible_array_concat_error()
}

fn concatenate_rank_mismatched_arrays(
    outer: ArrayValue,
    inner: ArrayValue,
    prepend: bool,
) -> Result<ArrayValue, ExecError> {
    if outer.ndim() != inner.ndim() + 1 || outer.dimensions[1..] != inner.dimensions[..] {
        return incompatible_array_concat_error();
    }
    let mut dimensions = outer.dimensions.clone();
    dimensions[0].length += 1;
    let mut elements = Vec::with_capacity(outer.elements.len() + inner.elements.len());
    if prepend {
        elements.extend(inner.elements);
        elements.extend(outer.elements);
    } else {
        elements.extend(outer.elements);
        elements.extend(inner.elements);
    }
    Ok(ArrayValue {
        element_type_oid: outer.element_type_oid.or(inner.element_type_oid),
        dimensions,
        elements,
    })
}

fn incompatible_array_concat_error<T>() -> Result<T, ExecError> {
    Err(ExecError::DetailedError {
        message: "cannot concatenate incompatible arrays".into(),
        detail: None,
        hint: None,
        sqlstate: "2202E",
    })
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

fn array_element_type_oid_from_value(value: &Value) -> Option<u32> {
    match value {
        Value::Record(record) if record.type_oid() != 0 => Some(record.type_oid()),
        Value::PgArray(array) => array.element_type_oid,
        _ => None,
    }
}
