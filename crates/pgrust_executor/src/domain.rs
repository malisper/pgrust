use pgrust_nodes::{SqlType, Value};

use crate::BooleanConstraintResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainConstraintLookupKind {
    Check,
    NotNull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainConstraintLookup {
    pub name: String,
    pub kind: DomainConstraintLookupKind,
    pub expr: Option<String>,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainLookup {
    pub oid: u32,
    pub name: String,
    pub sql_type: SqlType,
    pub not_null: bool,
    pub check: Option<String>,
    pub constraints: Vec<DomainConstraintLookup>,
}

pub trait DomainConstraintRuntime {
    type Error;

    fn domain_by_type_oid(&self, domain_oid: u32) -> Option<DomainLookup>;

    fn evaluate_domain_check(
        &mut self,
        value: &Value,
        domain_sql_type: SqlType,
        check_expr: &str,
    ) -> Result<BooleanConstraintResult, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainConstraintError<E> {
    Runtime(E),
    NotNull {
        domain_name: String,
    },
    Check {
        domain_name: String,
        constraint_name: String,
    },
    NonBoolCheck {
        domain_name: String,
        constraint_name: String,
    },
}

impl<E> DomainConstraintError<E> {
    pub fn map_runtime<F>(self, map: impl FnOnce(E) -> F) -> DomainConstraintError<F> {
        match self {
            DomainConstraintError::Runtime(err) => DomainConstraintError::Runtime(map(err)),
            DomainConstraintError::NotNull { domain_name } => {
                DomainConstraintError::NotNull { domain_name }
            }
            DomainConstraintError::Check {
                domain_name,
                constraint_name,
            } => DomainConstraintError::Check {
                domain_name,
                constraint_name,
            },
            DomainConstraintError::NonBoolCheck {
                domain_name,
                constraint_name,
            } => DomainConstraintError::NonBoolCheck {
                domain_name,
                constraint_name,
            },
        }
    }
}

pub fn enforce_domain_constraints_for_value<R>(
    value: Value,
    ty: SqlType,
    runtime: &mut R,
) -> Result<Value, DomainConstraintError<R::Error>>
where
    R: DomainConstraintRuntime,
{
    enforce_domain_constraints_for_value_ref(&value, ty, runtime)?;
    Ok(value)
}

pub fn enforce_domain_constraints_for_value_ref<R>(
    value: &Value,
    ty: SqlType,
    runtime: &mut R,
) -> Result<(), DomainConstraintError<R::Error>>
where
    R: DomainConstraintRuntime,
{
    enforce_domain_constraints_for_value_ref_as(value, ty, runtime, None)
}

fn enforce_domain_constraints_for_value_ref_as<R>(
    value: &Value,
    ty: SqlType,
    runtime: &mut R,
    outer_domain_name: Option<&str>,
) -> Result<(), DomainConstraintError<R::Error>>
where
    R: DomainConstraintRuntime,
{
    let Some(domain) = runtime.domain_by_type_oid(ty.type_oid) else {
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
                        runtime,
                        outer_domain_name,
                    )?;
                }
            }
            Value::Array(elements) => {
                for element in elements {
                    enforce_domain_constraints_for_value_ref_as(
                        element,
                        ty.element_type(),
                        runtime,
                        outer_domain_name,
                    )?;
                }
            }
            _ => {}
        }
        return Ok(());
    }

    let violation_domain_name = outer_domain_name.unwrap_or(&domain.name).to_string();
    if domain.sql_type.type_oid != 0 && domain.sql_type.type_oid != domain.oid {
        enforce_domain_constraints_for_value_ref_as(
            value,
            domain.sql_type,
            runtime,
            Some(&violation_domain_name),
        )?;
    }

    if domain.not_null && matches!(value, Value::Null) {
        return Err(DomainConstraintError::NotNull {
            domain_name: violation_domain_name,
        });
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
                        .map(|expr| (constraint.name.as_str(), expr.as_str()))
                })
                .flatten()
        })
        .map(|(name, expr)| (name.to_string(), expr.to_string()))
        .collect::<Vec<_>>();
    if checks.is_empty()
        && let Some(check) = domain.check.as_ref()
    {
        checks.push((domain.name.clone(), check.clone()));
    }

    for (constraint_name, check_expr) in checks {
        match runtime
            .evaluate_domain_check(value, domain.sql_type, &check_expr)
            .map_err(DomainConstraintError::Runtime)?
        {
            BooleanConstraintResult::Pass => {}
            BooleanConstraintResult::Fail => {
                return Err(DomainConstraintError::Check {
                    domain_name: violation_domain_name,
                    constraint_name,
                });
            }
            BooleanConstraintResult::NonBool => {
                return Err(DomainConstraintError::NonBoolCheck {
                    domain_name: domain.name,
                    constraint_name,
                });
            }
        }
    }

    Ok(())
}

pub fn domain_check_violation_message(domain_name: &str, constraint_name: &str) -> String {
    format!("value for domain {domain_name} violates check constraint \"{constraint_name}\"")
}

pub fn domain_not_null_violation_message(domain_name: &str) -> String {
    format!("domain {domain_name} does not allow null values")
}

pub fn domain_non_bool_check_detail(domain_name: &str, constraint_name: &str) -> String {
    format!(
        "constraint \"{constraint_name}\" on domain \"{domain_name}\" produced a non-boolean value"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::SqlTypeKind;

    struct TestRuntime {
        domains: Vec<DomainLookup>,
        result: BooleanConstraintResult,
    }

    impl DomainConstraintRuntime for TestRuntime {
        type Error = ();

        fn domain_by_type_oid(&self, domain_oid: u32) -> Option<DomainLookup> {
            self.domains
                .iter()
                .find(|domain| domain.oid == domain_oid)
                .cloned()
        }

        fn evaluate_domain_check(
            &mut self,
            _value: &Value,
            _domain_sql_type: SqlType,
            _check_expr: &str,
        ) -> Result<BooleanConstraintResult, Self::Error> {
            Ok(self.result)
        }
    }

    fn int_type(type_oid: u32) -> SqlType {
        SqlType::new(SqlTypeKind::Int4).with_identity(type_oid, 0)
    }

    #[test]
    fn not_null_domain_rejects_null() {
        let mut runtime = TestRuntime {
            domains: vec![DomainLookup {
                oid: 10,
                name: "positive_int".into(),
                sql_type: int_type(23),
                not_null: true,
                check: None,
                constraints: Vec::new(),
            }],
            result: BooleanConstraintResult::Pass,
        };

        let err =
            enforce_domain_constraints_for_value_ref(&Value::Null, int_type(10), &mut runtime)
                .expect_err("domain should reject null");
        assert_eq!(
            err,
            DomainConstraintError::NotNull {
                domain_name: "positive_int".into()
            }
        );
    }

    #[test]
    fn check_domain_reports_named_constraint() {
        let mut runtime = TestRuntime {
            domains: vec![DomainLookup {
                oid: 10,
                name: "positive_int".into(),
                sql_type: int_type(23),
                not_null: false,
                check: None,
                constraints: vec![DomainConstraintLookup {
                    name: "positive_int_check".into(),
                    kind: DomainConstraintLookupKind::Check,
                    expr: Some("VALUE > 0".into()),
                    enforced: true,
                }],
            }],
            result: BooleanConstraintResult::Fail,
        };

        let err =
            enforce_domain_constraints_for_value_ref(&Value::Int32(-1), int_type(10), &mut runtime)
                .expect_err("domain should reject value");
        assert_eq!(
            err,
            DomainConstraintError::Check {
                domain_name: "positive_int".into(),
                constraint_name: "positive_int_check".into()
            }
        );
    }
}
