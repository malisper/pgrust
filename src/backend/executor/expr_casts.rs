use super::ExecError;
use crate::backend::libpq::pqformat::{format_exec_error, format_exec_error_hint};
use crate::backend::parser::{CatalogLookup, DomainConstraintLookupKind, SqlType, SqlTypeKind};
use crate::backend::utils::misc::guc_datetime::{DateTimeConfig, IntervalStyle};
use crate::backend::utils::time::datetime::DateTimeParseError;
use crate::include::nodes::datum::{IntervalValue, NumericValue, Value};
use pgrust_catalog_data::*;

// :HACK: Keep the historical root executor module path while scalar cast
// implementation lives in `pgrust_expr`.
pub(crate) struct InputErrorInfo {
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) sqlstate: &'static str,
}

fn map_result<T>(result: Result<T, pgrust_expr::ExprError>) -> Result<T, ExecError> {
    result.map_err(Into::into)
}

fn map_input_error_info(info: pgrust_expr::expr_casts::InputErrorInfo) -> InputErrorInfo {
    InputErrorInfo {
        message: info.message,
        detail: info.detail,
        hint: info.hint,
        sqlstate: info.sqlstate,
    }
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

pub(crate) fn numeric_input_would_overflow(text: &str) -> bool {
    pgrust_expr::expr_casts::numeric_input_would_overflow(text)
}

pub(crate) fn invalid_interval_text_error(text: &str) -> ExecError {
    pgrust_expr::expr_casts::invalid_interval_text_error(text).into()
}

pub(crate) fn render_interval_text(value: IntervalValue) -> String {
    pgrust_expr::expr_casts::render_interval_text(value)
}

pub(crate) fn render_interval_text_with_config(
    value: IntervalValue,
    config: &DateTimeConfig,
) -> String {
    pgrust_expr::expr_casts::render_interval_text_with_config(value, config)
}

pub(crate) fn parse_interval_text_value(text: &str) -> Result<IntervalValue, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_interval_text_value(text))
}

pub(crate) fn parse_interval_text_value_with_style(
    text: &str,
    style: IntervalStyle,
) -> Result<IntervalValue, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_interval_text_value_with_style(text, style))
}

pub(crate) fn canonicalize_interval_text(text: &str) -> Result<String, ExecError> {
    map_result(pgrust_expr::expr_casts::canonicalize_interval_text(text))
}

pub(crate) fn parse_bytea_text(text: &str) -> Result<Vec<u8>, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_bytea_text(text))
}

pub fn render_internal_char_text(byte: u8) -> String {
    pgrust_expr::expr_casts::render_internal_char_text(byte)
}

pub(crate) fn parse_text_array_literal(
    raw: &str,
    element_type: SqlType,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_text_array_literal(
        raw,
        element_type,
    ))
}

pub(crate) fn parse_text_array_literal_with_op(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_text_array_literal_with_op(
        raw,
        element_type,
        op,
    ))
}

pub(crate) fn parse_text_array_literal_with_catalog_and_op(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::parse_text_array_literal_with_catalog_and_op(
                raw,
                element_type,
                op,
                catalog,
            ),
        )
    })
}

pub(crate) fn parse_text_array_literal_with_catalog_op_and_explicit(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::parse_text_array_literal_with_catalog_op_and_explicit(
                raw,
                element_type,
                op,
                explicit,
                catalog,
            ),
        )
    })
}

pub(crate) fn parse_text_array_literal_with_options(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
) -> Result<Value, ExecError> {
    map_result(
        pgrust_expr::expr_casts::parse_text_array_literal_with_options(
            raw,
            element_type,
            op,
            explicit,
        ),
    )
}

pub(crate) fn parse_composite_literal_fields(text: &str) -> Result<Vec<Option<String>>, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_composite_literal_fields(
        text,
    ))
}

pub(crate) fn render_pg_lsn_text(value: u64) -> String {
    pgrust_expr::expr_casts::render_pg_lsn_text(value)
}

pub(crate) fn pg_lsn_out_of_range() -> ExecError {
    pgrust_expr::expr_casts::pg_lsn_out_of_range().into()
}

pub(crate) fn parse_pg_lsn_text(text: &str) -> Result<u64, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_pg_lsn_text(text))
}

pub(crate) fn datetime_parse_error_details(
    ty: &'static str,
    text: &str,
    err: DateTimeParseError,
) -> String {
    pgrust_expr::expr_casts::datetime_parse_error_details(ty, text, err)
}

fn input_error_sqlstate(err: &ExecError) -> &'static str {
    match err {
        ExecError::JsonInput { sqlstate, .. }
        | ExecError::XmlInput { sqlstate, .. }
        | ExecError::DetailedError { sqlstate, .. }
        | ExecError::DiagnosticError { sqlstate, .. } => sqlstate,
        ExecError::InvalidIntegerInput { .. }
        | ExecError::ArrayInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidUuidInput { .. }
        | ExecError::InvalidGeometryInput { .. }
        | ExecError::InvalidBitInput { .. }
        | ExecError::InvalidBooleanInput { .. } => "22P02",
        ExecError::InvalidByteaHexDigit { .. } | ExecError::InvalidByteaHexOddDigits { .. } => {
            "22023"
        }
        ExecError::InvalidStorageValue { column, details }
            if matches!(
                column.as_str(),
                "date" | "time" | "timetz" | "timestamp" | "timestamptz"
            ) =>
        {
            if details.starts_with("time zone \"") {
                "22023"
            } else if details.starts_with("date/time field value out of range:")
                || details.starts_with("date out of range:")
            {
                "22008"
            } else {
                "22007"
            }
        }
        ExecError::InvalidStorageValue { column, .. }
            if matches!(column.as_str(), "inet" | "cidr") =>
        {
            "22P02"
        }
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath" && is_jsonpath_parse_error_details(details) =>
        {
            "42601"
        }
        ExecError::BitStringLengthMismatch { .. } => "22026",
        ExecError::BitStringTooLong { .. } => "22001",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow
        | ExecError::NumericFieldOverflow => "22003",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::InvalidFloatInput { .. } => "22P02",
        _ => "XX000",
    }
}

fn is_jsonpath_parse_error_details(details: &str) -> bool {
    details == "syntax error at end of jsonpath input"
        || details == "LAST is allowed only in array subscripts"
        || details == "@ is not allowed in root expressions"
        || details.starts_with("syntax error at or near ")
            && details.ends_with(" of jsonpath input")
        || details.starts_with("trailing junk after numeric literal at or near ")
            && details.ends_with(" of jsonpath input")
        || details.starts_with("invalid numeric literal at or near ")
            && details.ends_with(" of jsonpath input")
}

pub(crate) fn input_error_info(err: ExecError, _text: &str) -> InputErrorInfo {
    match err {
        ExecError::JsonInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        ExecError::XmlInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => InputErrorInfo {
            message,
            detail,
            hint,
            sqlstate,
        },
        ExecError::ArrayInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        other => InputErrorInfo {
            message: format_exec_error(&other),
            detail: None,
            hint: format_exec_error_hint(&other),
            sqlstate: input_error_sqlstate(&other),
        },
    }
}

pub(crate) fn soft_input_error_info(
    text: &str,
    type_name: &str,
) -> Result<Option<InputErrorInfo>, ExecError> {
    map_result(pgrust_expr::expr_casts::soft_input_error_info(
        text, type_name,
    ))
    .map(|info| info.map(map_input_error_info))
}

pub(crate) fn soft_input_error_info_with_config(
    text: &str,
    type_name: &str,
    config: &DateTimeConfig,
) -> Result<Option<InputErrorInfo>, ExecError> {
    map_result(pgrust_expr::expr_casts::soft_input_error_info_with_config(
        text, type_name, config,
    ))
    .map(|info| info.map(map_input_error_info))
}

pub(crate) fn soft_input_error_info_with_catalog_and_config(
    text: &str,
    type_name: &str,
    catalog: Option<&dyn CatalogLookup>,
    config: &DateTimeConfig,
) -> Result<Option<InputErrorInfo>, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::soft_input_error_info_with_catalog_and_config(
                text, type_name, catalog, config,
            ),
        )
        .map(|info| info.map(map_input_error_info))
    })
}

pub(crate) fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::cast_value(value, ty))
}

pub(crate) fn cast_value_with_config(
    value: Value,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::cast_value_with_config(
        value, ty, config,
    ))
}

pub(crate) fn cast_value_with_source_type_and_config(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    map_result(
        pgrust_expr::expr_casts::cast_value_with_source_type_and_config(
            value,
            source_type,
            ty,
            config,
        ),
    )
}

pub(crate) fn enforce_domain_constraints_for_value(
    value: Value,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::enforce_domain_constraints_for_value(value, ty, catalog),
        )
    })
}

pub(crate) fn cast_value_with_source_type_catalog_and_config(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::cast_value_with_source_type_catalog_and_config(
                value,
                source_type,
                ty,
                catalog,
                config,
            ),
        )
    })
}

pub(crate) fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::cast_text_value(text, ty, explicit))
}

pub(crate) fn cast_text_value_with_config(
    text: &str,
    ty: SqlType,
    explicit: bool,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::cast_text_value_with_config(
        text, ty, explicit, config,
    ))
}

pub(crate) fn cast_text_value_with_catalog_and_config(
    text: &str,
    ty: SqlType,
    explicit: bool,
    catalog: Option<&dyn CatalogLookup>,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        map_result(
            pgrust_expr::expr_casts::cast_text_value_with_catalog_and_config(
                text, ty, explicit, catalog, config,
            ),
        )
    })
}

pub(crate) fn cast_numeric_value(
    value: NumericValue,
    ty: SqlType,
    explicit: bool,
) -> Result<Value, ExecError> {
    map_result(pgrust_expr::expr_casts::cast_numeric_value(
        value, ty, explicit,
    ))
}

pub(crate) fn parse_uuid_text(text: &str) -> Result<[u8; 16], ExecError> {
    map_result(pgrust_expr::expr_casts::parse_uuid_text(text))
}

pub(crate) fn parse_pg_float(text: &str, kind: SqlTypeKind) -> Result<f64, ExecError> {
    map_result(pgrust_expr::expr_casts::parse_pg_float(text, kind))
}
