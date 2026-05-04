// :HACK: root compatibility shim while reg* lookup/format helpers live in
// `pgrust_expr`; keep root `ExecError` in the old signatures.
use super::ExecError;
use crate::backend::parser::{CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{PgOperatorRow, PgProcRow};
use crate::include::nodes::datum::Value;

struct ExprCatalogAdapter<'a>(&'a dyn CatalogLookup);

impl pgrust_expr::ExprCatalogLookup for ExprCatalogAdapter<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<pgrust_expr::BoundRelation> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::lookup_any_relation(self.0, name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_expr::BoundRelation> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::lookup_relation_by_oid(
            self.0,
            relation_oid,
        )
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_expr::BoundRelation> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::relation_by_oid(self.0, relation_oid)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<crate::include::catalog::PgClassRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::class_row_by_oid(
            self.0,
            relation_oid,
        )
    }

    fn authid_rows(&self) -> Vec<crate::include::catalog::PgAuthIdRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::authid_rows(self.0)
    }

    fn namespace_rows(&self) -> Vec<crate::include::catalog::PgNamespaceRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::namespace_rows(self.0)
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<crate::include::catalog::PgNamespaceRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::namespace_row_by_oid(self.0, oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::proc_rows_by_name(self.0, name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::proc_row_by_oid(self.0, oid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::operator_by_name_left_right(
            self.0,
            name,
            left_type_oid,
            right_type_oid,
        )
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::operator_by_oid(self.0, oid)
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::operator_rows(self.0)
    }

    fn collation_rows(&self) -> Vec<crate::include::catalog::PgCollationRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::collation_rows(self.0)
    }

    fn ts_config_rows(&self) -> Vec<crate::include::catalog::PgTsConfigRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::ts_config_rows(self.0)
    }

    fn ts_dict_rows(&self) -> Vec<crate::include::catalog::PgTsDictRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::ts_dict_rows(self.0)
    }

    fn type_rows(&self) -> Vec<crate::include::catalog::PgTypeRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::type_rows(self.0)
    }

    fn type_by_oid(&self, oid: u32) -> Option<crate::include::catalog::PgTypeRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::type_by_oid(self.0, oid)
    }

    fn type_by_name(&self, name: &str) -> Option<crate::include::catalog::PgTypeRow> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::type_by_name(self.0, name)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        <dyn CatalogLookup as pgrust_expr::ExprCatalogLookup>::type_oid_for_sql_type(
            self.0, sql_type,
        )
    }
}

fn with_expr_catalog<T>(
    catalog: Option<&dyn CatalogLookup>,
    f: impl FnOnce(Option<&dyn pgrust_expr::ExprCatalogLookup>) -> T,
) -> T {
    let adapter = catalog.map(ExprCatalogAdapter);
    f(adapter
        .as_ref()
        .map(|adapter| adapter as &dyn pgrust_expr::ExprCatalogLookup))
}

fn with_required_expr_catalog<T>(
    catalog: &dyn CatalogLookup,
    f: impl FnOnce(&dyn pgrust_expr::ExprCatalogLookup) -> T,
) -> T {
    let adapter = ExprCatalogAdapter(catalog);
    f(&adapter)
}

pub(crate) fn quote_identifier_if_needed(identifier: &str) -> String {
    pgrust_expr::expr_reg::quote_identifier_if_needed(identifier)
}

pub(crate) fn parse_sql_name_parts(input: &str) -> Result<Vec<String>, ExecError> {
    pgrust_expr::expr_reg::parse_sql_name_parts(input).map_err(Into::into)
}

pub(crate) fn resolve_regproc_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regproc_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regprocedure_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regprocedure_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regoper_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regoper_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regoperator_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regoperator_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regclass_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regclass_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn regclass_lookup_error(input: &str, catalog: Option<&dyn CatalogLookup>) -> ExecError {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::regclass_lookup_error(input, catalog).into()
    })
}

pub(crate) fn resolve_regtype_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regtype_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regrole_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regrole_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regnamespace_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regnamespace_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regcollation_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regcollation_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regconfig_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regconfig_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_regdictionary_oid(
    input: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_regdictionary_oid(input, catalog).map_err(Into::into)
    })
}

pub(crate) fn resolve_reg_object_oid(
    input: &str,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<u32, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::resolve_reg_object_oid(input, kind, catalog).map_err(Into::into)
    })
}

pub(crate) fn is_hard_regtype_input_error(err: &ExecError) -> bool {
    match err {
        ExecError::WithContext { context, .. } => context.starts_with("invalid type name "),
        ExecError::DetailedError { message, .. } => {
            message == "invalid NUMERIC type modifier"
                || message.starts_with("improper qualified name (too many dotted names): ")
                || message.starts_with("cross-database references are not implemented: ")
        }
        _ => false,
    }
}

pub(crate) fn cast_text_to_reg_object(
    input: &str,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::cast_text_to_reg_object(input, kind, catalog).map_err(Into::into)
    })
}

pub(crate) fn to_reg_object(
    value: &Value,
    kind: SqlTypeKind,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::to_reg_object(value, kind, catalog).map_err(Into::into)
    })
}

pub(crate) fn to_regtypemod(value: &Value, catalog: Option<&dyn CatalogLookup>) -> Value {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::to_regtypemod(value, catalog)
    })
}

pub(crate) fn format_type(
    oid: Option<u32>,
    typmod: Option<i32>,
    catalog: &dyn CatalogLookup,
) -> Value {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_type(oid, typmod, catalog)
    })
}

pub(crate) fn format_type_optional(
    oid: Option<u32>,
    typmod: Option<i32>,
    catalog: Option<&dyn CatalogLookup>,
) -> Value {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_type_optional(oid, typmod, catalog)
    })
}

pub(crate) fn format_type_text(
    oid: u32,
    typmod: Option<i32>,
    catalog: &dyn CatalogLookup,
) -> String {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_type_text(oid, typmod, catalog)
    })
}

pub(crate) fn format_regproc_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regproc_oid(oid, catalog)
    })
}

pub(crate) fn format_regproc_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regproc_oid_optional(oid, catalog)
    })
}

pub(crate) fn format_regprocedure_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regprocedure_oid(oid, catalog)
    })
}

pub(crate) fn format_regprocedure_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regprocedure_oid_optional(oid, catalog)
    })
}

pub(crate) fn format_regoper_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regoper_oid(oid, catalog)
    })
}

pub(crate) fn format_regoper_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regoper_oid_optional(oid, catalog)
    })
}

pub(crate) fn format_regoperator_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regoperator_oid(oid, catalog)
    })
}

pub(crate) fn format_regoperator_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regoperator_oid_optional(oid, catalog)
    })
}

pub(crate) fn format_regcollation_oid(oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regcollation_oid(oid, catalog)
    })
}

pub(crate) fn format_regcollation_oid_optional(
    oid: u32,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    with_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::format_regcollation_oid_optional(oid, catalog)
    })
}

pub(crate) fn function_signature_text(proc_row: &PgProcRow, catalog: &dyn CatalogLookup) -> String {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::function_signature_text(proc_row, catalog)
    })
}

pub(crate) fn operator_signature_text(
    operator_row: &PgOperatorRow,
    catalog: &dyn CatalogLookup,
) -> String {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::operator_signature_text(operator_row, catalog)
    })
}

pub(crate) fn type_oid_to_sql_type(oid: u32, catalog: &dyn CatalogLookup) -> Option<SqlType> {
    with_required_expr_catalog(catalog, |catalog| {
        pgrust_expr::expr_reg::type_oid_to_sql_type(oid, catalog)
    })
}
