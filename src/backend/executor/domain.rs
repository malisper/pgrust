use super::exec_expr::eval_expr;
use super::expr_casts::cast_text_value_with_catalog_and_config;
use super::{ExecError, ExecutorContext};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::parser::{
    SqlType, bind_expr_with_outer_and_ctes, parse_expr, scope_for_relation,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::RelationDesc;
use pgrust_executor::{
    BooleanConstraintResult, DomainConstraintError, DomainConstraintLookup,
    DomainConstraintLookupKind, DomainConstraintRuntime, DomainLookup,
    domain_check_violation_message, domain_non_bool_check_detail,
    domain_not_null_violation_message,
};

pub(crate) fn enforce_domain_constraints_for_value(
    value: Value,
    ty: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let mut runtime = RootDomainRuntime { ctx };
    pgrust_executor::enforce_domain_constraints_for_value_with_runtime(value, ty, &mut runtime)
        .map_err(domain_constraint_error)
}

pub(crate) fn enforce_domain_constraints_for_value_ref(
    value: &Value,
    ty: SqlType,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let mut runtime = RootDomainRuntime { ctx };
    pgrust_executor::enforce_domain_constraints_for_value_ref_with_runtime(value, ty, &mut runtime)
        .map_err(domain_constraint_error)
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
        message: domain_check_violation_message(domain_name, constraint_name),
        detail: None,
        hint: None,
        sqlstate: "23514",
    }
}

pub(crate) fn domain_not_null_violation(domain_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: domain_not_null_violation_message(domain_name),
        detail: None,
        hint: None,
        sqlstate: "23502",
    }
}

struct RootDomainRuntime<'a> {
    ctx: &'a mut ExecutorContext,
}

impl DomainConstraintRuntime for RootDomainRuntime<'_> {
    type Error = ExecError;

    fn domain_by_type_oid(&self, domain_oid: u32) -> Option<DomainLookup> {
        self.ctx
            .catalog
            .as_deref()
            .and_then(|catalog| catalog.domain_by_type_oid(domain_oid))
            .map(|domain| DomainLookup {
                oid: domain.oid,
                name: domain.name,
                sql_type: domain.sql_type,
                not_null: domain.not_null,
                check: domain.check,
                constraints: domain
                    .constraints
                    .into_iter()
                    .map(|constraint| DomainConstraintLookup {
                        name: constraint.name,
                        kind: match constraint.kind {
                            crate::backend::parser::DomainConstraintLookupKind::Check => {
                                DomainConstraintLookupKind::Check
                            }
                            crate::backend::parser::DomainConstraintLookupKind::NotNull => {
                                DomainConstraintLookupKind::NotNull
                            }
                        },
                        expr: constraint.expr,
                        enforced: constraint.enforced,
                    })
                    .collect(),
            })
    }

    fn evaluate_domain_check(
        &mut self,
        value: &Value,
        domain_sql_type: SqlType,
        check_expr: &str,
    ) -> Result<BooleanConstraintResult, Self::Error> {
        let raw = parse_expr(check_expr).map_err(ExecError::Parse)?;
        let desc = RelationDesc {
            columns: vec![column_desc("value", domain_sql_type, true)],
        };
        let scope = scope_for_relation(None, &desc);
        let bound = {
            let Some(catalog) = self.ctx.catalog.as_deref() else {
                return Ok(BooleanConstraintResult::Pass);
            };
            bind_expr_with_outer_and_ctes(&raw, &scope, catalog, &[], None, &[])
                .map_err(ExecError::Parse)?
        };
        let mut slot = TupleSlot::virtual_row(vec![value.clone()]);
        Ok(match eval_expr(&bound, &mut slot, self.ctx)? {
            Value::Null | Value::Bool(true) => BooleanConstraintResult::Pass,
            Value::Bool(false) => BooleanConstraintResult::Fail,
            _ => BooleanConstraintResult::NonBool,
        })
    }
}

fn domain_constraint_error(err: DomainConstraintError<ExecError>) -> ExecError {
    match err {
        DomainConstraintError::Runtime(err) => err,
        DomainConstraintError::NotNull { domain_name } => domain_not_null_violation(&domain_name),
        DomainConstraintError::Check {
            domain_name,
            constraint_name,
        } => domain_check_violation(&domain_name, &constraint_name),
        DomainConstraintError::NonBoolCheck {
            domain_name,
            constraint_name,
        } => ExecError::DetailedError {
            message: "CHECK constraint expression must return boolean".into(),
            detail: Some(domain_non_bool_check_detail(&domain_name, &constraint_name)),
            hint: None,
            sqlstate: "42804",
        },
    }
}
