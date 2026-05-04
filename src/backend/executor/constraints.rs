use crate::backend::executor::eval_expr;
use crate::backend::executor::value_io::format_failing_row_detail_for_columns;
use crate::backend::parser::BoundRelationConstraints;
use crate::backend::rewrite::RlsWriteCheck;
use crate::include::access::htup::ItemPointerData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::RelationDesc;
use pgrust_executor::{
    BooleanConstraintResult, NotNullConstraintDescriptor, RlsDetailSource, RlsWriteCheckSource,
    check_constraint_failure, find_not_null_violation, rls_write_check_failure,
    row_security_new_row_tid,
};

use super::{ExecError, ExecutorContext};

pub(crate) fn enforce_relation_constraints(
    relation_name: &str,
    desc: &RelationDesc,
    constraints: &BoundRelationConstraints,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let not_nulls = constraints
        .not_nulls
        .iter()
        .map(|constraint| NotNullConstraintDescriptor {
            column_index: constraint.column_index,
            constraint_name: constraint.constraint_name.clone(),
        })
        .collect::<Vec<_>>();
    if let Some(violation) =
        find_not_null_violation(relation_name, &desc.columns, &not_nulls, values)
    {
        return Err(ExecError::NotNullViolation {
            relation: relation_name.to_string(),
            column: violation.column,
            constraint: violation.constraint,
            detail: Some(format_failing_row_detail_for_columns(
                values,
                &desc.columns,
                &ctx.datetime_config,
            )),
        });
    }

    if constraints.checks.is_empty() {
        return Ok(());
    }

    let mut slot =
        TupleSlot::virtual_row_with_metadata(values.to_vec(), None, constraints.relation_oid);
    for check in &constraints.checks {
        if !check.enforced {
            continue;
        }
        match eval_expr(&check.expr, &mut slot, ctx)? {
            Value::Null | Value::Bool(true) => {}
            Value::Bool(false) => {
                return Err(ExecError::CheckViolation {
                    relation: relation_name.to_string(),
                    constraint: check.constraint_name.clone(),
                    detail: Some(format_failing_row_detail_for_columns(
                        values,
                        &desc.columns,
                        &ctx.datetime_config,
                    )),
                });
            }
            _ => {
                let failure = check_constraint_failure(
                    relation_name,
                    &check.constraint_name,
                    BooleanConstraintResult::NonBool,
                )
                .expect("non-boolean check result must produce a failure");
                return Err(ExecError::DetailedError {
                    message: failure.message,
                    detail: failure.detail,
                    hint: None,
                    sqlstate: failure.sqlstate,
                });
            }
        }
    }

    Ok(())
}

pub(crate) fn enforce_row_security_write_checks(
    relation_name: &str,
    desc: &RelationDesc,
    checks: &[RlsWriteCheck],
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    enforce_row_security_write_checks_with_tid(
        relation_name,
        desc,
        checks,
        values,
        Some(row_security_new_row_tid()),
        ctx,
    )
}

pub(crate) fn enforce_row_security_write_checks_with_tid(
    relation_name: &str,
    desc: &RelationDesc,
    checks: &[RlsWriteCheck],
    values: &[Value],
    row_tid: Option<ItemPointerData>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if checks.is_empty() {
        return Ok(());
    }

    let mut slot = TupleSlot::virtual_row_with_metadata(values.to_vec(), row_tid, None);
    for check in checks {
        let result = match eval_expr(&check.expr, &mut slot, ctx)? {
            Value::Bool(true) => BooleanConstraintResult::Pass,
            Value::Null | Value::Bool(false) => BooleanConstraintResult::Fail,
            _ => BooleanConstraintResult::NonBool,
        };
        if let Some(failure) = rls_write_check_failure(
            relation_name,
            check.policy_name.as_deref(),
            &root_rls_source(&check.source),
            !check.display_exprs.is_empty(),
            result,
        ) {
            let (message, static_detail, detail_source, sqlstate) = failure.split_static_detail();
            let detail = match detail_source {
                RlsDetailSource::BaseRow => Some(format_failing_row_detail_for_columns(
                    values,
                    &desc.columns,
                    &ctx.datetime_config,
                )),
                RlsDetailSource::DisplayExpressions => {
                    let mut display_values = check
                        .display_exprs
                        .iter()
                        .map(|expr| {
                            eval_expr(expr, &mut slot, ctx).map(|value| value.to_owned_value())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Value::materialize_all(&mut display_values);
                    Some(
                        crate::backend::executor::value_io::format_failing_row_detail(
                            &display_values,
                            &ctx.datetime_config,
                        ),
                    )
                }
                RlsDetailSource::None => static_detail,
            };
            return Err(ExecError::DetailedError {
                message,
                detail,
                hint: None,
                sqlstate,
            });
        }
    }

    Ok(())
}

fn root_rls_source(source: &crate::backend::rewrite::RlsWriteCheckSource) -> RlsWriteCheckSource {
    match source {
        crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(view_name) => {
            RlsWriteCheckSource::ViewCheckOption(view_name.clone())
        }
        crate::backend::rewrite::RlsWriteCheckSource::ConflictUpdateVisibility => {
            RlsWriteCheckSource::ConflictUpdateVisibility
        }
        crate::backend::rewrite::RlsWriteCheckSource::MergeUpdateVisibility => {
            RlsWriteCheckSource::MergeUpdateVisibility
        }
        crate::backend::rewrite::RlsWriteCheckSource::MergeDeleteVisibility => {
            RlsWriteCheckSource::MergeDeleteVisibility
        }
        crate::backend::rewrite::RlsWriteCheckSource::Insert
        | crate::backend::rewrite::RlsWriteCheckSource::Update
        | crate::backend::rewrite::RlsWriteCheckSource::SelectVisibility => {
            RlsWriteCheckSource::Policy
        }
    }
}
