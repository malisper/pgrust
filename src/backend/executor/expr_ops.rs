use std::cmp::Ordering;

use super::ExecError;
use super::node_types::*;
use crate::backend::parser::{CatalogLookup, DomainConstraintLookupKind, SqlType};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::nodes::datum::{IntervalValue, NumericValue, RecordValue};
use pgrust_catalog_data::*;

// :HACK: Keep the historical root executor module path while scalar operator
// implementation lives in `pgrust_expr`.
pub(crate) type TextCollationSemantics = pgrust_expr::expr_ops::TextCollationSemantics;

fn map_result<T>(result: Result<T, pgrust_expr::ExprError>) -> Result<T, ExecError> {
    result.map_err(Into::into)
}

struct RootExprCatalog<'a>(&'a dyn CatalogLookup);

fn expr_bound_relation(
    catalog: &dyn CatalogLookup,
    relation: crate::backend::parser::BoundRelation,
) -> pgrust_expr::BoundRelation {
    let name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    pgrust_expr::BoundRelation {
        relation_oid: relation.relation_oid,
        oid: Some(relation.relation_oid),
        name,
        relkind: relation.relkind,
        desc: relation.desc,
    }
}

impl pgrust_expr::ExprCatalogLookup for RootExprCatalog<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<pgrust_expr::BoundRelation> {
        self.0
            .lookup_any_relation(name)
            .map(|relation| expr_bound_relation(self.0, relation))
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_expr::BoundRelation> {
        self.0
            .lookup_relation_by_oid(relation_oid)
            .map(|relation| expr_bound_relation(self.0, relation))
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_expr::BoundRelation> {
        self.0
            .relation_by_oid(relation_oid)
            .map(|relation| expr_bound_relation(self.0, relation))
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.0.class_row_by_oid(relation_oid)
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.0.authid_rows()
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        self.0.namespace_rows()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        self.0.namespace_row_by_oid(oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        self.0.proc_rows_by_name(name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        self.0.proc_row_by_oid(oid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        self.0
            .operator_by_name_left_right(name, left_type_oid, right_type_oid)
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        self.0.operator_by_oid(oid)
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        self.0.operator_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        self.0.collation_rows()
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        self.0.ts_config_rows()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        self.0.ts_dict_rows()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        self.0.ts_config_map_rows()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        self.0.type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.0.type_by_oid(oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        self.0.type_by_name(name)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        self.0.type_oid_for_sql_type(sql_type)
    }

    fn domain_by_type_oid(&self, domain_oid: u32) -> Option<pgrust_expr::DomainLookup> {
        self.0
            .domain_by_type_oid(domain_oid)
            .map(|domain| pgrust_expr::DomainLookup {
                name: domain.name,
                sql_type: domain.sql_type,
                not_null: domain.not_null,
                check: domain.check,
                constraints: domain
                    .constraints
                    .into_iter()
                    .map(|constraint| pgrust_expr::DomainConstraintLookup {
                        name: constraint.name,
                        kind: match constraint.kind {
                            DomainConstraintLookupKind::Check => {
                                pgrust_expr::DomainConstraintLookupKind::Check
                            }
                            DomainConstraintLookupKind::NotNull => {
                                pgrust_expr::DomainConstraintLookupKind::NotNull
                            }
                        },
                        expr: constraint.expr,
                        enforced: constraint.enforced,
                    })
                    .collect(),
            })
    }

    fn enum_label_oid(&self, type_oid: u32, label: &str) -> Option<u32> {
        self.0.enum_label_oid(type_oid, label)
    }

    fn enum_label(&self, type_oid: u32, label_oid: u32) -> Option<String> {
        self.0.enum_label(type_oid, label_oid)
    }

    fn enum_label_by_oid(&self, label_oid: u32) -> Option<String> {
        self.0.enum_label_by_oid(label_oid)
    }

    fn enum_label_is_committed(&self, type_oid: u32, label_oid: u32) -> bool {
        self.0.enum_label_is_committed(type_oid, label_oid)
    }

    fn domain_allowed_enum_label_oids(&self, domain_oid: u32) -> Option<Vec<u32>> {
        self.0.domain_allowed_enum_label_oids(domain_oid)
    }

    fn domain_check_name(&self, domain_oid: u32) -> Option<String> {
        self.0.domain_check_name(domain_oid)
    }
}

fn with_expr_catalog<T>(
    catalog: Option<&dyn CatalogLookup>,
    f: impl FnOnce(Option<&dyn pgrust_expr::ExprCatalogLookup>) -> T,
) -> T {
    match catalog {
        Some(catalog) => {
            let adapter = RootExprCatalog(catalog);
            f(Some(&adapter))
        }
        None => f(None),
    }
}

pub(crate) fn compare_order_by_keys(
    items: &[OrderByEntry],
    left_keys: &[Value],
    right_keys: &[Value],
) -> Result<Ordering, ExecError> {
    map_result(pgrust_expr::expr_ops::compare_order_by_keys(
        items, left_keys, right_keys,
    ))
}

pub(crate) fn compare_order_values(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
    nulls_first: Option<bool>,
    descending: bool,
) -> Result<Ordering, ExecError> {
    map_result(pgrust_expr::expr_ops::compare_order_values(
        left,
        right,
        collation_oid,
        nulls_first,
        descending,
    ))
}

pub(crate) fn eval_and(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::eval_and(left, right))
}

pub(crate) fn eval_or(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::eval_or(left, right))
}

pub(crate) fn compare_values(
    op: &'static str,
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::compare_values(
        op,
        left,
        right,
        collation_oid,
    ))
}

pub(crate) fn compare_values_with_type(
    op: &'static str,
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
    datetime_config: Option<&DateTimeConfig>,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::compare_values_with_type(
        op,
        left,
        left_type,
        right,
        right_type,
        collation_oid,
        datetime_config,
    ))
}

pub(crate) fn compare_values_with_type_and_catalog(
    op: &'static str,
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
    datetime_config: Option<&DateTimeConfig>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(pgrust_expr::expr_ops::compare_values_with_type_and_catalog(
            op,
            left,
            left_type,
            right,
            right_type,
            collation_oid,
            datetime_config,
            catalog,
        ))
    })
}

pub(crate) fn not_equal_values(
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::not_equal_values(
        left,
        right,
        collation_oid,
    ))
}

pub(crate) fn not_equal_values_with_type(
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
    datetime_config: Option<&DateTimeConfig>,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::not_equal_values_with_type(
        left,
        left_type,
        right,
        right_type,
        collation_oid,
        datetime_config,
    ))
}

pub(crate) fn not_equal_values_with_type_and_catalog(
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
    datetime_config: Option<&DateTimeConfig>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_ops::not_equal_values_with_type_and_catalog(
                left,
                left_type,
                right,
                right_type,
                collation_oid,
                datetime_config,
                catalog,
            ),
        )
    })
}

pub(crate) fn values_are_distinct(left: &Value, right: &Value) -> bool {
    pgrust_expr::expr_ops::values_are_distinct(left, right)
}

pub(crate) fn add_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::add_values(left, right))
}

pub(crate) fn add_values_with_config(
    left: Value,
    right: Value,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::add_values_with_config(
        left, right, config,
    ))
}

pub(crate) fn sub_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::sub_values(left, right))
}

pub(crate) fn sub_values_with_config(
    left: Value,
    right: Value,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::sub_values_with_config(
        left, right, config,
    ))
}

pub(crate) fn mixed_date_timestamp_ordering(
    left: &Value,
    right: &Value,
    config: Option<&DateTimeConfig>,
) -> Option<Ordering> {
    pgrust_expr::expr_ops::mixed_date_timestamp_ordering(left, right, config)
}

pub(crate) fn mul_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::mul_values(left, right))
}

pub(crate) fn shift_left_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::shift_left_values(left, right))
}

pub(crate) fn shift_right_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::shift_right_values(left, right))
}

pub(crate) fn bitwise_and_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::bitwise_and_values(left, right))
}

pub(crate) fn bitwise_or_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::bitwise_or_values(left, right))
}

pub(crate) fn bitwise_xor_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::bitwise_xor_values(left, right))
}

pub(crate) fn bitwise_not_value(value: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::bitwise_not_value(value))
}

pub(crate) fn div_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::div_values(left, right))
}

pub(crate) fn mod_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::mod_values(left, right))
}

pub(crate) fn concat_values(left: Value, right: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::concat_values(left, right))
}

pub(crate) fn concat_values_with_cast_context(
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(pgrust_expr::expr_ops::concat_values_with_cast_context(
            left, left_type, right, right_type, catalog, config,
        ))
    })
}

pub(crate) fn negate_value(value: Value) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::negate_value(value))
}

pub(crate) fn order_values(
    op: &'static str,
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::order_values(
        op,
        left,
        right,
        collation_oid,
    ))
}

pub(crate) fn ensure_builtin_collation_supported(
    collation_oid: Option<u32>,
) -> Result<(), ExecError> {
    map_result(pgrust_expr::expr_ops::ensure_builtin_collation_supported(
        collation_oid,
    ))
}

pub(crate) fn ensure_text_collation_supported(
    collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<(), ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(pgrust_expr::expr_ops::ensure_text_collation_supported(
            collation_oid,
            catalog,
        ))
    })
}

pub(crate) fn text_collation_semantics(
    collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<TextCollationSemantics, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(pgrust_expr::expr_ops::text_collation_semantics(
            collation_oid,
            catalog,
        ))
    })
}

pub(crate) fn compare_text_values_with_catalog(
    left: &str,
    right: &str,
    collation_oid: Option<u32>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Ordering, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(pgrust_expr::expr_ops::compare_text_values_with_catalog(
            left,
            right,
            collation_oid,
            catalog,
        ))
    })
}

pub(crate) fn interval_div_float(span: IntervalValue, factor: f64) -> Option<IntervalValue> {
    pgrust_expr::expr_ops::interval_div_float(span, factor)
}

pub(crate) fn order_record_image_values(
    op: &'static str,
    left: &RecordValue,
    right: &RecordValue,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_ops::order_record_image_values(
        op, left, right,
    ))
}

pub(crate) fn parse_numeric_text(text: &str) -> Option<NumericValue> {
    pgrust_expr::expr_ops::parse_numeric_text(text)
}
