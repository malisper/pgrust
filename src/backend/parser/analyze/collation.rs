use super::coerce::{is_text_like_type, sql_type_name};
use super::*;
use crate::include::catalog::DEFAULT_COLLATION_OID;
use crate::include::nodes::primnodes::expr_sql_type_hint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CollationConsumer {
    OrderBy,
    StringComparison,
    Like,
    ILike,
    Similar,
}

pub(crate) fn consumer_for_subquery_comparison_op(
    op: SubqueryComparisonOp,
) -> Option<CollationConsumer> {
    Some(match op {
        SubqueryComparisonOp::Eq
        | SubqueryComparisonOp::NotEq
        | SubqueryComparisonOp::Lt
        | SubqueryComparisonOp::LtEq
        | SubqueryComparisonOp::Gt
        | SubqueryComparisonOp::GtEq => CollationConsumer::StringComparison,
        SubqueryComparisonOp::Like | SubqueryComparisonOp::NotLike => CollationConsumer::Like,
        SubqueryComparisonOp::ILike | SubqueryComparisonOp::NotILike => CollationConsumer::ILike,
        SubqueryComparisonOp::RegexMatch | SubqueryComparisonOp::NotRegexMatch => {
            CollationConsumer::StringComparison
        }
        SubqueryComparisonOp::Similar | SubqueryComparisonOp::NotSimilar => {
            CollationConsumer::Similar
        }
        SubqueryComparisonOp::Match => return None,
    })
}

pub(crate) fn resolve_collation_oid(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let normalized = normalize_catalog_lookup_name(name);
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.collname.eq_ignore_ascii_case(normalized))
        .map(|row| row.oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known collation",
            actual: name.to_string(),
        })
}

pub(crate) fn is_collatable_type(ty: SqlType) -> bool {
    if ty.is_array {
        is_text_like_type(ty.element_type())
    } else {
        is_text_like_type(ty)
    }
}

pub(crate) fn default_collation_oid_for_type(ty: SqlType) -> Option<u32> {
    is_collatable_type(ty).then_some(DEFAULT_COLLATION_OID)
}

pub(crate) fn bind_explicit_collation(
    expr: Expr,
    sql_type: SqlType,
    collation: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Expr, ParseError> {
    if !is_collatable_type(sql_type) {
        return Err(ParseError::DetailedError {
            message: format!(
                "collations are not supported by type {}",
                sql_type_name(sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(Expr::Collate {
        expr: Box::new(expr),
        collation_oid: resolve_collation_oid(collation, catalog)?,
    })
}

pub(crate) fn strip_explicit_collation(expr: Expr) -> (Expr, Option<u32>) {
    match expr {
        Expr::Collate {
            expr,
            collation_oid,
        } => (*expr, Some(collation_oid)),
        other => (other, None),
    }
}

pub(crate) fn derive_consumer_collation(
    catalog: &dyn CatalogLookup,
    consumer: CollationConsumer,
    inputs: &[(SqlType, Option<u32>)],
) -> Result<Option<u32>, ParseError> {
    let mut explicit = None;
    let mut implicit = None;
    let mut saw_collatable = false;

    for (sql_type, explicit_oid) in inputs {
        if let Some(oid) = explicit_oid {
            saw_collatable = true;
            match explicit {
                Some(previous) if previous != *oid => {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "collation mismatch between explicit collations \"{}\" and \"{}\"",
                            collation_name(catalog, previous),
                            collation_name(catalog, *oid)
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P21",
                    });
                }
                None => explicit = Some(*oid),
                _ => {}
            }
            continue;
        }

        if let Some(implicit_oid) = default_collation_oid_for_type(*sql_type) {
            saw_collatable = true;
            match implicit {
                Some(previous) if previous != implicit_oid => {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "collation mismatch between implicit collations \"{}\" and \"{}\"",
                            collation_name(catalog, previous),
                            collation_name(catalog, implicit_oid)
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P21",
                    });
                }
                None => implicit = Some(implicit_oid),
                _ => {}
            }
        }
    }

    if explicit.is_some() {
        return Ok(explicit);
    }
    if implicit.is_some() {
        return Ok(implicit);
    }
    if !saw_collatable {
        return Ok(None);
    }

    Err(ParseError::DetailedError {
        message: no_collation_message(consumer).into(),
        detail: None,
        hint: None,
        sqlstate: "42P22",
    })
}

pub(crate) fn finalize_order_by_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, Option<u32>), ParseError> {
    let expr_type = expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let (expr, explicit_collation_oid) = strip_explicit_collation(expr);
    let collation_oid = derive_consumer_collation(
        catalog,
        CollationConsumer::OrderBy,
        &[(expr_type, explicit_collation_oid)],
    )?;
    Ok((expr, collation_oid))
}

fn collation_name(catalog: &dyn CatalogLookup, oid: u32) -> String {
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| row.collname)
        .unwrap_or_else(|| oid.to_string())
}

fn no_collation_message(consumer: CollationConsumer) -> &'static str {
    match consumer {
        CollationConsumer::OrderBy => "could not determine which collation to use for ORDER BY",
        CollationConsumer::StringComparison => {
            "could not determine which collation to use for string comparison"
        }
        CollationConsumer::Like => "could not determine which collation to use for LIKE",
        CollationConsumer::ILike => "could not determine which collation to use for ILIKE",
        CollationConsumer::Similar => {
            "could not determine which collation to use for regular expression"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::ParseError;
    use crate::include::catalog::{C_COLLATION_OID, POSIX_COLLATION_OID};
    use crate::include::nodes::datum::Value;

    struct TestCatalog;

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }
    }

    #[test]
    fn resolve_builtin_collation_oids() {
        let catalog = TestCatalog;
        assert_eq!(resolve_collation_oid("default", &catalog).unwrap(), 100);
        assert_eq!(resolve_collation_oid("C", &catalog).unwrap(), 950);
        assert_eq!(resolve_collation_oid("POSIX", &catalog).unwrap(), 951);
        assert_eq!(
            resolve_collation_oid("pg_catalog.default", &catalog).unwrap(),
            100
        );
    }

    #[test]
    fn reject_unknown_collation_name() {
        let catalog = TestCatalog;
        assert!(matches!(
            resolve_collation_oid("missing_collation", &catalog),
            Err(ParseError::UnexpectedToken { expected, actual })
                if expected == "known collation" && actual == "missing_collation"
        ));
    }

    #[test]
    fn reject_explicit_collation_on_noncollatable_type() {
        let catalog = TestCatalog;
        assert!(matches!(
            bind_explicit_collation(
                Expr::Const(Value::Int32(1)),
                SqlType::new(SqlTypeKind::Int4),
                "C",
                &catalog,
            ),
            Err(ParseError::DetailedError { message, sqlstate, .. })
                if message == "collations are not supported by type integer"
                    && sqlstate == "42804"
        ));
    }

    #[test]
    fn reject_mismatched_explicit_collations() {
        let catalog = TestCatalog;
        assert!(matches!(
            derive_consumer_collation(
                &catalog,
                CollationConsumer::StringComparison,
                &[
                    (SqlType::new(SqlTypeKind::Text), Some(C_COLLATION_OID)),
                    (SqlType::new(SqlTypeKind::Text), Some(POSIX_COLLATION_OID)),
                ],
            ),
            Err(ParseError::DetailedError { message, sqlstate, .. })
                if message
                    == "collation mismatch between explicit collations \"C\" and \"POSIX\""
                    && sqlstate == "42P21"
        ));
    }
}
