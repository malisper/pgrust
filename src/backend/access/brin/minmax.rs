// :HACK: root compatibility shim while BRIN minmax runtime moves into `pgrust_access`.
use pgrust_access::brin::minmax as access_minmax;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::access::brin_internal::BrinValues;
use crate::include::nodes::datum::Value;

pub(crate) use access_minmax::BrinMinmaxStrategy;

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub(crate) fn minmax_strategy_from_i16(value: i16) -> Result<BrinMinmaxStrategy, CatalogError> {
    BrinMinmaxStrategy::try_from(value).map_err(catalog_error)
}

pub(crate) fn compare_minmax_values(
    left: &Value,
    right: &Value,
) -> Result<std::cmp::Ordering, CatalogError> {
    catalog_result(access_minmax::compare_minmax_values(
        left,
        right,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_multi_add_value(
    column: &mut BrinValues,
    new_value: &Value,
    is_null: bool,
) -> Result<bool, CatalogError> {
    catalog_result(access_minmax::minmax_multi_add_value(
        column,
        new_value,
        is_null,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_multi_consistent(
    column: &BrinValues,
    strategy: BrinMinmaxStrategy,
    scan_value: &Value,
) -> Result<bool, CatalogError> {
    catalog_result(access_minmax::minmax_multi_consistent(
        column,
        strategy,
        scan_value,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_multi_union(
    left: &mut BrinValues,
    right: &BrinValues,
) -> Result<(), CatalogError> {
    catalog_result(access_minmax::minmax_multi_union(
        left,
        right,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_opcinfo(proc_oid: u32) -> Result<(usize, bool), CatalogError> {
    catalog_result(access_minmax::minmax_opcinfo(proc_oid))
}

pub(crate) fn minmax_add_value(
    proc_oid: u32,
    column: &mut BrinValues,
    new_value: &Value,
    is_null: bool,
) -> Result<bool, CatalogError> {
    catalog_result(access_minmax::minmax_add_value(
        proc_oid,
        column,
        new_value,
        is_null,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_consistent(
    proc_oid: u32,
    column: &BrinValues,
    strategy: BrinMinmaxStrategy,
    scan_value: &Value,
) -> Result<bool, CatalogError> {
    catalog_result(access_minmax::minmax_consistent(
        proc_oid,
        column,
        strategy,
        scan_value,
        &RootAccessServices,
    ))
}

pub(crate) fn minmax_union(
    proc_oid: u32,
    left: &mut BrinValues,
    right: &BrinValues,
) -> Result<(), CatalogError> {
    catalog_result(access_minmax::minmax_union(
        proc_oid,
        left,
        right,
        &RootAccessServices,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{
        BRIN_MINMAX_ADD_VALUE_PROC_OID, BRIN_MINMAX_CONSISTENT_PROC_OID,
        BRIN_MINMAX_OPCINFO_PROC_OID, BRIN_MINMAX_UNION_PROC_OID,
    };

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
    fn minmax_multi_eliminates_values_between_disjoint_points() {
        let mut column = BrinValues {
            attno: 1,
            has_nulls: false,
            all_nulls: true,
            values: vec![Value::Null; 8],
        };

        minmax_multi_add_value(&mut column, &Value::Int32(1000), false).unwrap();
        minmax_multi_add_value(&mut column, &Value::Int32(2000), false).unwrap();
        minmax_multi_add_value(&mut column, &Value::Int32(1_000_000), false).unwrap();

        assert!(
            minmax_multi_consistent(&column, BrinMinmaxStrategy::Equal, &Value::Int32(1000))
                .unwrap()
        );
        assert!(
            !minmax_multi_consistent(&column, BrinMinmaxStrategy::Equal, &Value::Int32(500_000))
                .unwrap()
        );
        assert!(
            minmax_multi_consistent(&column, BrinMinmaxStrategy::Greater, &Value::Int32(500_000))
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
