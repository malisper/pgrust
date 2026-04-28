use super::exec_expr::eval_expr;
use super::expr_casts::cast_text_value_with_catalog_and_config;
use super::{ExecError, ExecutorContext};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::parser::{
    DomainConstraintLookupKind, SqlType, bind_expr_with_outer_and_ctes, parse_expr,
    scope_for_relation,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) fn enforce_domain_constraints_for_value(
    value: Value,
    ty: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    enforce_domain_constraints_for_value_ref(&value, ty, ctx)?;
    Ok(value)
}

pub(crate) fn enforce_domain_constraints_for_value_ref(
    value: &Value,
    ty: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    enforce_domain_constraints_for_value_ref_as(value, ty, ctx, None)
}

fn enforce_domain_constraints_for_value_ref_as(
    value: &Value,
    ty: SqlType,
    ctx: &mut ExecutorContext,
    outer_domain_name: Option<&str>,
) -> Result<(), ExecError> {
    let Some(domain) = ctx
        .catalog
        .as_deref()
        .and_then(|catalog| catalog.domain_by_type_oid(ty.type_oid))
    else {
        return Ok(());
    };

    if ty.is_array && !domain.sql_type.is_array {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        match value {
            Value::PgArray(array) => {
                for element in &array.elements {
                    enforce_domain_constraints_for_value_ref_as(
                        element,
                        ty.element_type(),
                        ctx,
                        outer_domain_name,
                    )?;
                }
            }
            Value::Array(elements) => {
                for element in elements {
                    enforce_domain_constraints_for_value_ref_as(
                        element,
                        ty.element_type(),
                        ctx,
                        outer_domain_name,
                    )?;
                }
            }
            _ => {}
        }
        return Ok(());
    }

    let violation_domain_name = outer_domain_name.unwrap_or(&domain.name);
    if domain.sql_type.type_oid != 0 && domain.sql_type.type_oid != domain.oid {
        enforce_domain_constraints_for_value_ref_as(
            value,
            domain.sql_type,
            ctx,
            Some(violation_domain_name),
        )?;
    }

    if domain.not_null && matches!(value, Value::Null) {
        return Err(domain_not_null_violation(violation_domain_name));
    }
    if matches!(value, Value::Null) {
        return Ok(());
    }

    let mut checks = domain
        .constraints
        .iter()
        .filter_map(|constraint| {
            (constraint.enforced && matches!(constraint.kind, DomainConstraintLookupKind::Check))
                .then(|| {
                    constraint
                        .expr
                        .as_ref()
                        .map(|expr| (&constraint.name, expr))
                })
                .flatten()
        })
        .collect::<Vec<_>>();
    if checks.is_empty()
        && let Some(check) = domain.check.as_ref()
    {
        checks.push((&domain.name, check));
    }

    for (constraint_name, check) in checks {
        let raw = parse_expr(check).map_err(ExecError::Parse)?;
        let desc = RelationDesc {
            columns: vec![column_desc("value", domain.sql_type, true)],
        };
        let scope = scope_for_relation(None, &desc);
        let bound = {
            let Some(catalog) = ctx.catalog.as_deref() else {
                return Ok(());
            };
            bind_expr_with_outer_and_ctes(&raw, &scope, catalog, &[], None, &[])
                .map_err(ExecError::Parse)?
        };
        let mut slot = TupleSlot::virtual_row(vec![value.clone()]);
        match eval_expr(&bound, &mut slot, ctx)? {
            Value::Null | Value::Bool(true) => {}
            Value::Bool(false) => {
                return Err(domain_check_violation(
                    violation_domain_name,
                    constraint_name,
                ));
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: "CHECK constraint expression must return boolean".into(),
                    detail: Some(format!(
                        "constraint \"{}\" on domain \"{}\" produced a non-boolean value",
                        constraint_name, domain.name
                    )),
                    hint: None,
                    sqlstate: "42804",
                });
            }
        }
    }

    Ok(())
}

pub(crate) fn cast_domain_text_input(
    text: &str,
    ty: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<Option<Value>, ExecError> {
    let Some(domain) = ctx
        .catalog
        .as_deref()
        .and_then(|catalog| catalog.domain_by_type_oid(ty.type_oid))
    else {
        return Ok(None);
    };
    let Some(catalog) = ctx.catalog.clone() else {
        return Ok(None);
    };
    let value = cast_text_value_with_catalog_and_config(
        text,
        domain.sql_type,
        false,
        Some(catalog.as_ref()),
        &ctx.datetime_config,
    )?;
    enforce_domain_constraints_for_value(value, ty, ctx).map(Some)
}

pub(crate) fn domain_check_violation(domain_name: &str, constraint_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "value for domain {domain_name} violates check constraint \"{constraint_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    }
}

pub(crate) fn domain_not_null_violation(domain_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("domain {domain_name} does not allow null values"),
        detail: None,
        hint: None,
        sqlstate: "23502",
    }
}
