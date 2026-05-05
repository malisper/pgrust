use super::coerce::{is_text_like_type, sql_type_name};
use super::*;
use pgrust_catalog_data::DEFAULT_COLLATION_OID;
use pgrust_nodes::primnodes::{OpExprKind, expr_sql_type_hint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollationConsumer {
    OrderBy,
    StringComparison,
    Like,
    ILike,
    Similar,
}

pub fn consumer_for_subquery_comparison_op(op: SubqueryComparisonOp) -> Option<CollationConsumer> {
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

pub fn resolve_collation_oid(name: &str, catalog: &dyn CatalogLookup) -> Result<u32, ParseError> {
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

pub fn is_collatable_type(ty: SqlType) -> bool {
    if ty.is_array {
        is_text_like_type(ty.element_type())
    } else {
        is_text_like_type(ty)
    }
}

pub fn default_collation_oid_for_type(ty: SqlType) -> Option<u32> {
    is_collatable_type(ty).then_some(DEFAULT_COLLATION_OID)
}

pub fn bind_explicit_collation(
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

pub fn strip_explicit_collation(expr: Expr) -> (Expr, Option<u32>) {
    match expr {
        Expr::Collate {
            expr,
            collation_oid,
        } => (*expr, Some(collation_oid)),
        other => (other, None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedCollation {
    None,
    Default(u32),
    Implicit(u32),
    Explicit(u32),
    Conflict {
        left: u32,
        right: u32,
        explicit: bool,
    },
}

fn combine_expr_collations(left: DerivedCollation, right: DerivedCollation) -> DerivedCollation {
    use DerivedCollation::*;

    match (left, right) {
        (conflict @ Conflict { .. }, _) | (_, conflict @ Conflict { .. }) => conflict,
        (Explicit(left), Explicit(right)) if left != right => Conflict {
            left,
            right,
            explicit: true,
        },
        (Explicit(oid), _) | (_, Explicit(oid)) => Explicit(oid),
        (Implicit(left), Implicit(right)) if left == DEFAULT_COLLATION_OID => Implicit(right),
        (Implicit(left), Implicit(right)) if right == DEFAULT_COLLATION_OID => Implicit(left),
        (Implicit(left), Implicit(right)) if left != right => Conflict {
            left,
            right,
            explicit: false,
        },
        (Implicit(oid), _) | (_, Implicit(oid)) => Implicit(oid),
        (Default(oid), Default(_)) | (Default(oid), None) | (None, Default(oid)) => Default(oid),
        (None, None) => None,
    }
}

fn combine_many_expr_collations(
    collations: impl IntoIterator<Item = DerivedCollation>,
) -> DerivedCollation {
    collations
        .into_iter()
        .fold(DerivedCollation::None, combine_expr_collations)
}

fn default_if_no_collation(collation: DerivedCollation) -> DerivedCollation {
    match collation {
        DerivedCollation::None => DerivedCollation::Default(DEFAULT_COLLATION_OID),
        other => other,
    }
}

pub fn derive_expr_collation(expr: &Expr, sql_type: SqlType) -> DerivedCollation {
    if !is_collatable_type(sql_type) {
        return DerivedCollation::None;
    }

    match expr {
        Expr::Collate { collation_oid, .. } => DerivedCollation::Explicit(*collation_oid),
        Expr::Var(var) => var
            .collation_oid
            .map(DerivedCollation::Implicit)
            .unwrap_or(DerivedCollation::Default(DEFAULT_COLLATION_OID)),
        Expr::Cast(inner, target_type) => {
            default_if_no_collation(derive_expr_collation(inner, *target_type))
        }
        Expr::Coalesce(left, right) => combine_expr_collations(
            derive_expr_collation(left, expr_sql_type_hint(left).unwrap_or(sql_type)),
            derive_expr_collation(right, expr_sql_type_hint(right).unwrap_or(sql_type)),
        ),
        Expr::Case(case_expr) => combine_many_expr_collations(
            case_expr
                .args
                .iter()
                .map(|when| {
                    derive_expr_collation(
                        &when.result,
                        expr_sql_type_hint(&when.result).unwrap_or(case_expr.casetype),
                    )
                })
                .chain(std::iter::once(derive_expr_collation(
                    &case_expr.defresult,
                    expr_sql_type_hint(&case_expr.defresult).unwrap_or(case_expr.casetype),
                ))),
        ),
        Expr::Func(func) => func
            .collation_oid
            .map(DerivedCollation::Implicit)
            .unwrap_or_else(|| {
                default_if_no_collation(combine_many_expr_collations(func.args.iter().map(|arg| {
                    derive_expr_collation(arg, expr_sql_type_hint(arg).unwrap_or(sql_type))
                })))
            }),
        Expr::Op(op) => op
            .collation_oid
            .map(DerivedCollation::Implicit)
            .unwrap_or_else(|| {
                if matches!(
                    op.op,
                    OpExprKind::Concat | OpExprKind::JsonGetText | OpExprKind::JsonPathText
                ) {
                    default_if_no_collation(combine_many_expr_collations(op.args.iter().map(
                        |arg| {
                            derive_expr_collation(arg, expr_sql_type_hint(arg).unwrap_or(sql_type))
                        },
                    )))
                } else {
                    DerivedCollation::Default(DEFAULT_COLLATION_OID)
                }
            }),
        Expr::SubLink(sublink) => sublink
            .subselect
            .target_list
            .first()
            .map(|target| derive_expr_collation(&target.expr, target.sql_type))
            .unwrap_or(DerivedCollation::Default(DEFAULT_COLLATION_OID)),
        Expr::FieldSelect {
            expr, field_type, ..
        } => derive_expr_collation(expr, *field_type),
        Expr::ArraySubscript { array, .. } => {
            derive_expr_collation(array, expr_sql_type_hint(array).unwrap_or(sql_type))
        }
        _ => DerivedCollation::Default(DEFAULT_COLLATION_OID),
    }
}

pub fn derive_consumer_collation_from_exprs(
    catalog: &dyn CatalogLookup,
    consumer: CollationConsumer,
    inputs: &[(&Expr, SqlType, Option<u32>)],
) -> Result<Option<u32>, ParseError> {
    let derived = inputs.iter().fold(DerivedCollation::None, |acc, input| {
        let (expr, sql_type, explicit_oid) = *input;
        let input_collation = explicit_oid
            .map(DerivedCollation::Explicit)
            .unwrap_or_else(|| derive_expr_collation(expr, sql_type));
        combine_expr_collations(acc, input_collation)
    });

    match derived {
        DerivedCollation::None => Ok(None),
        DerivedCollation::Default(oid)
        | DerivedCollation::Implicit(oid)
        | DerivedCollation::Explicit(oid) => Ok(Some(oid)),
        DerivedCollation::Conflict {
            left,
            right,
            explicit,
        } => Err(collation_mismatch_error(catalog, left, right, explicit)),
    }
    .and_then(|oid| {
        if oid.is_none()
            && inputs
                .iter()
                .any(|(_, sql_type, _)| default_collation_oid_for_type(*sql_type).is_some())
        {
            Err(ParseError::DetailedError {
                message: no_collation_message(consumer).into(),
                detail: None,
                hint: None,
                sqlstate: "42P22",
            })
        } else {
            Ok(oid)
        }
    })
}

pub fn derive_consumer_collation(
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

pub fn finalize_order_by_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, Option<u32>), ParseError> {
    let (expr, explicit_collation_oid) = strip_explicit_collation(expr);
    let Some(expr_type) = expr_sql_type_hint(&expr) else {
        return Ok((expr, explicit_collation_oid));
    };
    let collation_oid = derive_consumer_collation_from_exprs(
        catalog,
        CollationConsumer::OrderBy,
        &[(&expr, expr_type, explicit_collation_oid)],
    )?;
    Ok((expr, collation_oid))
}

fn collation_mismatch_error(
    catalog: &dyn CatalogLookup,
    left: u32,
    right: u32,
    explicit: bool,
) -> ParseError {
    ParseError::DetailedError {
        message: format!(
            "collation mismatch between {} collations \"{}\" and \"{}\"",
            if explicit { "explicit" } else { "implicit" },
            collation_name(catalog, left),
            collation_name(catalog, right)
        ),
        detail: None,
        hint: None,
        sqlstate: "42P21",
    }
}

pub fn collation_name(catalog: &dyn CatalogLookup, oid: u32) -> String {
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
    use pgrust_catalog_data::{C_COLLATION_OID, POSIX_COLLATION_OID};
    use pgrust_nodes::datum::Value;
    use pgrust_nodes::parsenodes::ParseError;
    use pgrust_nodes::primnodes::{FuncExpr, ScalarFunctionImpl};

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

    #[test]
    fn text_function_without_collatable_args_uses_default_collation() {
        let expr = Expr::Func(Box::new(FuncExpr {
            funcid: 1,
            funcname: Some("test_text_func".into()),
            funcresulttype: Some(SqlType::new(SqlTypeKind::Text)),
            funcvariadic: false,
            implementation: ScalarFunctionImpl::UserDefined { proc_oid: 1 },
            collation_oid: None,
            display_args: None,
            args: vec![Expr::Const(Value::Int32(1))],
        }));

        assert_eq!(
            derive_expr_collation(&expr, SqlType::new(SqlTypeKind::Text)),
            DerivedCollation::Default(DEFAULT_COLLATION_OID)
        );
    }

    #[test]
    fn text_cast_from_noncollatable_type_uses_default_collation() {
        let expr = Expr::Cast(
            Box::new(Expr::Const(Value::Int32(1))),
            SqlType::new(SqlTypeKind::Text),
        );

        assert_eq!(
            derive_expr_collation(&expr, SqlType::new(SqlTypeKind::Text)),
            DerivedCollation::Default(DEFAULT_COLLATION_OID)
        );
    }

    #[test]
    fn order_by_finalization_does_not_invent_text_type() {
        let catalog = TestCatalog;

        assert_eq!(
            finalize_order_by_expr(Expr::Random, &catalog).unwrap(),
            (Expr::Random, None)
        );
    }
}
