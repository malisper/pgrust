use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::compare_order_values;
use crate::include::access::brin_internal::BrinValues;
use crate::include::catalog::{
    BRIN_MINMAX_ADD_VALUE_PROC_OID, BRIN_MINMAX_CONSISTENT_PROC_OID, BRIN_MINMAX_OPCINFO_PROC_OID,
    BRIN_MINMAX_UNION_PROC_OID,
};
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrinMinmaxStrategy {
    Less = 1,
    LessEqual = 2,
    Equal = 3,
    GreaterEqual = 4,
    Greater = 5,
}

impl TryFrom<i16> for BrinMinmaxStrategy {
    type Error = CatalogError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Less),
            2 => Ok(Self::LessEqual),
            3 => Ok(Self::Equal),
            4 => Ok(Self::GreaterEqual),
            5 => Ok(Self::Greater),
            _ => Err(CatalogError::Io(format!(
                "unsupported BRIN minmax strategy {value}"
            ))),
        }
    }
}

fn ensure_support_proc(actual: u32, expected: u32, label: &str) -> Result<(), CatalogError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CatalogError::Io(format!(
            "unsupported BRIN minmax {label} proc {actual}"
        )))
    }
}

fn ensure_summary_shape(column: &BrinValues) -> Result<(), CatalogError> {
    if column.values.len() == 2 {
        Ok(())
    } else {
        Err(CatalogError::Corrupt(
            "BRIN minmax summary columns must store exactly two values",
        ))
    }
}

fn compare_minmax_values(left: &Value, right: &Value) -> Result<Ordering, CatalogError> {
    match (left, right) {
        (Value::Int16(a), Value::Int16(b)) => Ok(a.cmp(b)),
        (Value::InternalChar(a), Value::InternalChar(b)) => Ok(a.cmp(b)),
        (Value::Null, _) | (_, Value::Null) => Err(CatalogError::Corrupt(
            "BRIN minmax comparisons cannot use NULL values",
        )),
        _ => compare_order_values(left, right, None, Some(false), false)
            .map_err(|err| CatalogError::Io(format!("BRIN minmax comparison failed: {err:?}"))),
    }
}

pub(crate) fn minmax_opcinfo(proc_oid: u32) -> Result<(usize, bool), CatalogError> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_OPCINFO_PROC_OID, "opcinfo")?;
    Ok((2, true))
}

pub(crate) fn minmax_add_value(
    proc_oid: u32,
    column: &mut BrinValues,
    new_value: &Value,
    is_null: bool,
) -> Result<bool, CatalogError> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_ADD_VALUE_PROC_OID, "add_value")?;
    ensure_summary_shape(column)?;
    if is_null || matches!(new_value, Value::Null) {
        return Err(CatalogError::Corrupt(
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
    if compare_minmax_values(new_value, &column.values[0])? == Ordering::Less {
        column.values[0] = new_value.clone();
        updated = true;
    }
    if compare_minmax_values(new_value, &column.values[1])? == Ordering::Greater {
        column.values[1] = new_value.clone();
        updated = true;
    }
    Ok(updated)
}

pub(crate) fn minmax_consistent(
    proc_oid: u32,
    column: &BrinValues,
    strategy: BrinMinmaxStrategy,
    scan_value: &Value,
) -> Result<bool, CatalogError> {
    ensure_support_proc(proc_oid, BRIN_MINMAX_CONSISTENT_PROC_OID, "consistent")?;
    ensure_summary_shape(column)?;
    if column.all_nulls {
        return Ok(false);
    }

    match strategy {
        BrinMinmaxStrategy::Less => {
            Ok(compare_minmax_values(&column.values[0], scan_value)? == Ordering::Less)
        }
        BrinMinmaxStrategy::LessEqual => {
            Ok(compare_minmax_values(&column.values[0], scan_value)? != Ordering::Greater)
        }
        BrinMinmaxStrategy::Equal => {
            let min_matches =
                compare_minmax_values(&column.values[0], scan_value)? != Ordering::Greater;
            let max_matches =
                compare_minmax_values(&column.values[1], scan_value)? != Ordering::Less;
            Ok(min_matches && max_matches)
        }
        BrinMinmaxStrategy::GreaterEqual => {
            Ok(compare_minmax_values(&column.values[1], scan_value)? != Ordering::Less)
        }
        BrinMinmaxStrategy::Greater => {
            Ok(compare_minmax_values(&column.values[1], scan_value)? == Ordering::Greater)
        }
    }
}

pub(crate) fn minmax_union(
    proc_oid: u32,
    left: &mut BrinValues,
    right: &BrinValues,
) -> Result<(), CatalogError> {
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

    if compare_minmax_values(&right.values[0], &left.values[0])? == Ordering::Less {
        left.values[0] = right.values[0].clone();
    }
    if compare_minmax_values(&right.values[1], &left.values[1])? == Ordering::Greater {
        left.values[1] = right.values[1].clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary() -> BrinValues {
        BrinValues {
            attno: 1,
            has_nulls: false,
            all_nulls: true,
            values: vec![Value::Null, Value::Null],
        }
    }

    #[test]
    fn minmax_opcinfo_uses_pg_shape() {
        let (nstored, regular_nulls) = minmax_opcinfo(BRIN_MINMAX_OPCINFO_PROC_OID).unwrap();
        assert_eq!(nstored, 2);
        assert!(regular_nulls);
    }

    #[test]
    fn minmax_add_value_expands_bounds() {
        let mut column = summary();
        assert!(
            minmax_add_value(
                BRIN_MINMAX_ADD_VALUE_PROC_OID,
                &mut column,
                &Value::Int32(10),
                false,
            )
            .unwrap()
        );
        assert_eq!(column.values[0], Value::Int32(10));
        assert_eq!(column.values[1], Value::Int32(10));

        assert!(
            minmax_add_value(
                BRIN_MINMAX_ADD_VALUE_PROC_OID,
                &mut column,
                &Value::Int32(4),
                false,
            )
            .unwrap()
        );
        assert!(
            minmax_add_value(
                BRIN_MINMAX_ADD_VALUE_PROC_OID,
                &mut column,
                &Value::Int32(19),
                false,
            )
            .unwrap()
        );

        assert_eq!(column.values[0], Value::Int32(4));
        assert_eq!(column.values[1], Value::Int32(19));
    }

    #[test]
    fn minmax_consistent_checks_equality_and_ranges() {
        let mut column = summary();
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut column,
            &Value::Int32(4),
            false,
        )
        .unwrap();
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut column,
            &Value::Int32(19),
            false,
        )
        .unwrap();

        assert!(
            minmax_consistent(
                BRIN_MINMAX_CONSISTENT_PROC_OID,
                &column,
                BrinMinmaxStrategy::Equal,
                &Value::Int32(10),
            )
            .unwrap()
        );
        assert!(
            !minmax_consistent(
                BRIN_MINMAX_CONSISTENT_PROC_OID,
                &column,
                BrinMinmaxStrategy::Equal,
                &Value::Int32(30),
            )
            .unwrap()
        );
        assert!(
            minmax_consistent(
                BRIN_MINMAX_CONSISTENT_PROC_OID,
                &column,
                BrinMinmaxStrategy::GreaterEqual,
                &Value::Int32(19),
            )
            .unwrap()
        );
        assert!(
            !minmax_consistent(
                BRIN_MINMAX_CONSISTENT_PROC_OID,
                &column,
                BrinMinmaxStrategy::Less,
                &Value::Int32(2),
            )
            .unwrap()
        );
    }

    #[test]
    fn minmax_union_merges_non_null_bounds() {
        let mut left = summary();
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut left,
            &Value::Int16(3),
            false,
        )
        .unwrap();
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut left,
            &Value::Int16(8),
            false,
        )
        .unwrap();

        let mut right = summary();
        right.has_nulls = true;
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut right,
            &Value::Int16(1),
            false,
        )
        .unwrap();
        minmax_add_value(
            BRIN_MINMAX_ADD_VALUE_PROC_OID,
            &mut right,
            &Value::Int16(12),
            false,
        )
        .unwrap();

        minmax_union(BRIN_MINMAX_UNION_PROC_OID, &mut left, &right).unwrap();
        assert_eq!(left.values[0], Value::Int16(1));
        assert_eq!(left.values[1], Value::Int16(12));
        assert!(left.has_nulls);
    }

    #[test]
    fn minmax_handles_all_null_summaries() {
        let all_nulls = summary();
        assert!(
            !minmax_consistent(
                BRIN_MINMAX_CONSISTENT_PROC_OID,
                &all_nulls,
                BrinMinmaxStrategy::Equal,
                &Value::Text("x".into()),
            )
            .unwrap()
        );

        let mut left = summary();
        let right = summary();
        minmax_union(BRIN_MINMAX_UNION_PROC_OID, &mut left, &right).unwrap();
        assert!(left.all_nulls);
    }

    #[test]
    fn minmax_rejects_unknown_support_proc() {
        let err = minmax_opcinfo(999_999).unwrap_err();
        assert!(matches!(err, CatalogError::Io(_)));
    }
}
