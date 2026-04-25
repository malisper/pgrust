use crate::backend::executor::ExecError;
use crate::backend::parser::{
    CreateIndexStatement, CreateSchemaStatement, CreateSequenceStatement, CreateTableStatement,
    CreateTriggerStatement, CreateViewStatement, GrantObjectStatement, Statement,
};
use crate::include::catalog::PgNamespaceRow;
use crate::pgrust::auth::{AuthCatalog, AuthState};

pub(crate) struct ResolvedCreateSchema {
    pub schema_name: String,
    pub owner_oid: u32,
}

pub(crate) enum CreateSchemaResolution {
    Create(ResolvedCreateSchema),
    SkipExisting,
}

fn create_schema_mismatch_error(found: &str, target: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "CREATE specifies a schema ({found}) different from the one being created ({target})"
        ),
        detail: None,
        hint: None,
        sqlstate: "42P15",
    }
}

fn set_schema_name(schema_name: &mut Option<String>, target_schema: &str) -> Result<(), ExecError> {
    match schema_name {
        Some(found_schema) if !found_schema.eq_ignore_ascii_case(target_schema) => {
            Err(create_schema_mismatch_error(found_schema, target_schema))
        }
        Some(_) => Ok(()),
        None => {
            *schema_name = Some(target_schema.to_string());
            Ok(())
        }
    }
}

fn split_relation_name(name: &str) -> (Option<String>, String) {
    name.split_once('.')
        .map(|(schema_name, relation_name)| (Some(schema_name.to_string()), relation_name.into()))
        .unwrap_or_else(|| (None, name.into()))
}

fn normalize_create_sequence_element(
    mut stmt: CreateSequenceStatement,
    target_schema: &str,
) -> Result<Statement, ExecError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateSequence(stmt))
}

fn normalize_create_table_element(
    mut stmt: CreateTableStatement,
    target_schema: &str,
) -> Result<Statement, ExecError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateTable(stmt))
}

fn normalize_create_view_element(
    mut stmt: CreateViewStatement,
    target_schema: &str,
) -> Result<Statement, ExecError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateView(stmt))
}

fn normalize_create_index_element(
    mut stmt: CreateIndexStatement,
    target_schema: &str,
) -> Result<Statement, ExecError> {
    let (mut schema_name, relation_name) = split_relation_name(&stmt.table_name);
    set_schema_name(&mut schema_name, target_schema)?;
    stmt.table_name = relation_name;
    Ok(Statement::CreateIndex(stmt))
}

fn normalize_create_trigger_element(
    mut stmt: CreateTriggerStatement,
    target_schema: &str,
) -> Result<Statement, ExecError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateTrigger(stmt))
}

fn normalize_grant_element(stmt: GrantObjectStatement) -> Statement {
    Statement::GrantObject(stmt)
}

pub(crate) fn transform_create_schema_stmt_elements(
    elements: &[Box<Statement>],
    target_schema: &str,
) -> Result<Vec<Statement>, ExecError> {
    let mut sequences = Vec::new();
    let mut tables = Vec::new();
    let mut views = Vec::new();
    let mut indexes = Vec::new();
    let mut triggers = Vec::new();
    let mut grants = Vec::new();

    for element in elements {
        match element.as_ref() {
            Statement::CreateSequence(stmt) => {
                sequences.push(normalize_create_sequence_element(
                    stmt.clone(),
                    target_schema,
                )?);
            }
            Statement::CreateTable(stmt) => {
                tables.push(normalize_create_table_element(stmt.clone(), target_schema)?);
            }
            Statement::CreateView(stmt) => {
                views.push(normalize_create_view_element(stmt.clone(), target_schema)?);
            }
            Statement::CreateIndex(stmt) => {
                indexes.push(normalize_create_index_element(stmt.clone(), target_schema)?);
            }
            Statement::CreateTrigger(stmt) => {
                triggers.push(normalize_create_trigger_element(
                    stmt.clone(),
                    target_schema,
                )?);
            }
            Statement::GrantObject(stmt) => grants.push(normalize_grant_element(stmt.clone())),
            _ => {
                return Err(ExecError::Parse(
                    crate::backend::parser::ParseError::FeatureNotSupported(
                        "CREATE SCHEMA elements other than CREATE SEQUENCE, CREATE TABLE, CREATE VIEW, CREATE INDEX, CREATE TRIGGER, or GRANT".into(),
                    ),
                ));
            }
        }
    }

    let mut ordered = Vec::with_capacity(elements.len());
    ordered.extend(sequences);
    ordered.extend(tables);
    ordered.extend(views);
    ordered.extend(indexes);
    ordered.extend(triggers);
    ordered.extend(grants);
    Ok(ordered)
}

pub(crate) fn resolve_create_schema_stmt(
    stmt: &CreateSchemaStatement,
    auth: &AuthState,
    auth_catalog: &AuthCatalog,
    database_owner_oid: u32,
    has_database_create_privilege: bool,
    namespace_rows: &[PgNamespaceRow],
) -> Result<CreateSchemaResolution, ExecError> {
    let current_user = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .ok_or_else(|| ExecError::DetailedError {
            message: "current role does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;
    let target_owner = match stmt.auth_role.as_deref() {
        Some(role_name) => {
            auth_catalog
                .role_by_name(role_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("role \"{role_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?
        }
        None => current_user,
    };

    if target_owner.oid != auth.current_user_oid()
        && !auth.can_set_role(target_owner.oid, auth_catalog)
    {
        return Err(ExecError::DetailedError {
            message: format!("must be able to SET ROLE \"{}\"", target_owner.rolname),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    }

    if !current_user.rolsuper
        && auth.current_user_oid() != database_owner_oid
        && !has_database_create_privilege
    {
        return Err(ExecError::DetailedError {
            message: "permission denied for database postgres".into(),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    }

    let schema_name = stmt
        .schema_name
        .clone()
        .unwrap_or_else(|| target_owner.rolname.clone())
        .to_ascii_lowercase();

    if schema_name.starts_with("pg_") {
        return Err(ExecError::DetailedError {
            message: format!("unacceptable schema name \"{schema_name}\""),
            detail: Some("The prefix \"pg_\" is reserved for system schemas.".into()),
            hint: None,
            sqlstate: "42939",
        });
    }

    if namespace_rows
        .iter()
        .any(|row| row.nspname.eq_ignore_ascii_case(&schema_name))
    {
        return if stmt.if_not_exists {
            Ok(CreateSchemaResolution::SkipExisting)
        } else {
            Err(ExecError::DetailedError {
                message: format!("schema \"{schema_name}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42P06",
            })
        };
    }

    Ok(CreateSchemaResolution::Create(ResolvedCreateSchema {
        schema_name,
        owner_oid: target_owner.oid,
    }))
}
