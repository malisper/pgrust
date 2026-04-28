use crate::backend::executor::eval_expr;
use crate::backend::executor::value_io::format_failing_row_detail_for_columns;
use crate::backend::parser::BoundRelationConstraints;
use crate::backend::rewrite::RlsWriteCheck;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::RelationDesc;

use super::{ExecError, ExecutorContext};

pub(crate) fn enforce_relation_constraints(
    relation_name: &str,
    desc: &RelationDesc,
    constraints: &BoundRelationConstraints,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for (index, column) in desc.columns.iter().enumerate() {
        if column.storage.nullable {
            continue;
        }
        if matches!(values.get(index), Some(Value::Null) | None) {
            let constraint_name = constraints
                .not_nulls
                .iter()
                .find(|constraint| constraint.column_index == index)
                .map(|constraint| constraint.constraint_name.clone())
                .or_else(|| column.not_null_constraint_name.clone())
                .unwrap_or_else(|| format!("{relation_name}_{}_not_null", column.name));
            return Err(ExecError::NotNullViolation {
                relation: relation_name.to_string(),
                column: column.name.clone(),
                constraint: constraint_name,
                detail: Some(format_failing_row_detail_for_columns(
                    values,
                    &desc.columns,
                    &ctx.datetime_config,
                )),
            });
        }
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
                return Err(ExecError::DetailedError {
                    message: "CHECK constraint expression must return boolean".into(),
                    detail: Some(format!(
                        "constraint \"{}\" on relation \"{}\" produced a non-boolean value",
                        check.constraint_name, relation_name
                    )),
                    hint: None,
                    sqlstate: "42804",
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
    if checks.is_empty() {
        return Ok(());
    }

    let mut slot = TupleSlot::virtual_row(values.to_vec());
    for check in checks {
        match eval_expr(&check.expr, &mut slot, ctx)? {
            Value::Bool(true) => {}
            Value::Null | Value::Bool(false) => {
                if let crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(view_name) =
                    &check.source
                {
                    return Err(ExecError::DetailedError {
                        message: format!("new row violates check option for view \"{view_name}\""),
                        detail: Some(format_failing_row_detail_for_columns(
                            values,
                            &desc.columns,
                            &ctx.datetime_config,
                        )),
                        hint: None,
                        sqlstate: "44000",
                    });
                }
                if matches!(
                    check.source,
                    crate::backend::rewrite::RlsWriteCheckSource::ConflictUpdateVisibility
                ) {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "new row violates row-level security policy (USING expression) for table \"{relation_name}\""
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                if matches!(
                    check.source,
                    crate::backend::rewrite::RlsWriteCheckSource::MergeUpdateVisibility
                        | crate::backend::rewrite::RlsWriteCheckSource::MergeDeleteVisibility
                ) {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "target row violates row-level security policy (USING expression) for table \"{relation_name}\""
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                return Err(ExecError::DetailedError {
                    message: check
                        .policy_name
                        .as_ref()
                        .map(|policy_name| {
                            format!(
                                "new row violates row-level security policy \"{policy_name}\" for table \"{relation_name}\""
                            )
                        })
                        .unwrap_or_else(|| {
                            format!(
                                "new row violates row-level security policy for table \"{relation_name}\""
                            )
                        }),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            _ => {
                if let crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(view_name) =
                    &check.source
                {
                    return Err(ExecError::DetailedError {
                        message: "view CHECK OPTION expression must return boolean".into(),
                        detail: Some(format!(
                            "check option for view \"{view_name}\" produced a non-boolean value"
                        )),
                        hint: None,
                        sqlstate: "42804",
                    });
                }
                return Err(ExecError::DetailedError {
                    message: "row-level security policy expression must return boolean".into(),
                    detail: Some(
                        check
                            .policy_name
                            .as_ref()
                            .map(|policy_name| {
                                format!(
                                    "policy \"{policy_name}\" on relation \"{relation_name}\" produced a non-boolean value"
                                )
                            })
                            .unwrap_or_else(|| {
                                format!(
                                    "row-level security policy on relation \"{relation_name}\" produced a non-boolean value"
                                )
                            }),
                    ),
                    hint: None,
                    sqlstate: "42804",
                });
            }
        }
    }

    Ok(())
}
