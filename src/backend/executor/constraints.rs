use crate::backend::executor::eval_expr;
use crate::backend::parser::BoundRelationConstraints;
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
            });
        }
    }

    if constraints.checks.is_empty() {
        return Ok(());
    }

    let mut slot = TupleSlot::virtual_row(values.to_vec());
    for check in &constraints.checks {
        match eval_expr(&check.expr, &mut slot, ctx)? {
            Value::Null | Value::Bool(true) => {}
            Value::Bool(false) => {
                return Err(ExecError::CheckViolation {
                    relation: relation_name.to_string(),
                    constraint: check.constraint_name.clone(),
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
