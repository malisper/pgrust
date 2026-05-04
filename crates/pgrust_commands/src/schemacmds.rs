use pgrust_catalog_data::{PgAuthIdRow, PgNamespaceRow};
use pgrust_expr::ExprError;
use pgrust_nodes::parsenodes::{
    CreateIndexStatement, CreateSchemaStatement, CreateSequenceStatement, CreateTableStatement,
    CreateTriggerStatement, CreateViewStatement, GrantObjectStatement, ParseError, RoleSpec,
    Statement,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCreateSchema {
    pub schema_name: String,
    pub owner_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateSchemaResolution {
    Create(ResolvedCreateSchema),
    SkipExisting(String),
}

pub trait CreateSchemaAuthContext {
    fn current_user_oid(&self) -> u32;
    fn session_user_oid(&self) -> u32;
    fn role_by_oid(&self, oid: u32) -> Option<&PgAuthIdRow>;
    fn role_by_name(&self, name: &str) -> Option<&PgAuthIdRow>;
    fn can_set_role(&self, role_oid: u32) -> bool;
}

fn create_schema_mismatch_error(found: &str, target: &str) -> ExprError {
    ExprError::DetailedError {
        message: format!(
            "CREATE specifies a schema ({found}) different from the one being created ({target})"
        ),
        detail: None,
        hint: None,
        sqlstate: "42P15",
    }
}

fn set_schema_name(schema_name: &mut Option<String>, target_schema: &str) -> Result<(), ExprError> {
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
) -> Result<Statement, ExprError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateSequence(stmt))
}

fn normalize_create_table_element(
    mut stmt: CreateTableStatement,
    target_schema: &str,
) -> Result<Statement, ExprError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateTable(stmt))
}

fn normalize_create_view_element(
    mut stmt: CreateViewStatement,
    target_schema: &str,
) -> Result<Statement, ExprError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateView(stmt))
}

fn normalize_create_index_element(
    mut stmt: CreateIndexStatement,
    target_schema: &str,
) -> Result<Statement, ExprError> {
    let (mut schema_name, relation_name) = split_relation_name(&stmt.table_name);
    set_schema_name(&mut schema_name, target_schema)?;
    stmt.table_name = relation_name;
    Ok(Statement::CreateIndex(stmt))
}

fn normalize_create_trigger_element(
    mut stmt: CreateTriggerStatement,
    target_schema: &str,
) -> Result<Statement, ExprError> {
    set_schema_name(&mut stmt.schema_name, target_schema)?;
    Ok(Statement::CreateTrigger(stmt))
}

fn normalize_grant_element(stmt: GrantObjectStatement) -> Statement {
    Statement::GrantObject(stmt)
}

pub fn transform_create_schema_stmt_elements(
    elements: &[Box<Statement>],
    target_schema: &str,
) -> Result<Vec<Statement>, ExprError> {
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
                return Err(ExprError::Parse(ParseError::FeatureNotSupported(
                    "CREATE SCHEMA elements other than CREATE SEQUENCE, CREATE TABLE, CREATE VIEW, CREATE INDEX, CREATE TRIGGER, or GRANT".into(),
                )));
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

pub fn resolve_create_schema_stmt(
    stmt: &CreateSchemaStatement,
    auth: &impl CreateSchemaAuthContext,
    database_owner_oid: u32,
    has_database_create_privilege: bool,
    namespace_rows: &[PgNamespaceRow],
) -> Result<CreateSchemaResolution, ExprError> {
    let current_user =
        auth.role_by_oid(auth.current_user_oid())
            .ok_or_else(|| ExprError::DetailedError {
                message: "current role does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
    let target_owner = match stmt.auth_role.as_ref() {
        Some(RoleSpec::RoleName(role_name)) => {
            auth.role_by_name(role_name)
                .ok_or_else(|| ExprError::DetailedError {
                    message: format!("role \"{role_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?
        }
        Some(RoleSpec::CurrentUser | RoleSpec::CurrentRole) => current_user,
        Some(RoleSpec::SessionUser) => {
            auth.role_by_oid(auth.session_user_oid())
                .ok_or_else(|| ExprError::DetailedError {
                    message: "session user does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?
        }
        None => current_user,
    };

    if target_owner.oid != auth.current_user_oid() && !auth.can_set_role(target_owner.oid) {
        return Err(ExprError::DetailedError {
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
        return Err(ExprError::DetailedError {
            message: "permission denied for database postgres".into(),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    }

    let schema_name = stmt
        .schema_name
        .clone()
        .unwrap_or_else(|| target_owner.rolname.clone());

    if stmt.if_not_exists && !stmt.elements.is_empty() {
        return Err(ExprError::Parse(ParseError::DetailedError {
            message: "CREATE SCHEMA IF NOT EXISTS cannot include schema elements".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }));
    }

    if schema_name.starts_with("pg_") {
        return Err(ExprError::DetailedError {
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
            Ok(CreateSchemaResolution::SkipExisting(schema_name))
        } else {
            Err(ExprError::DetailedError {
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

#[cfg(test)]
mod tests {
    use super::*;

    struct TestAuth {
        current_user_oid: u32,
        session_user_oid: u32,
        roles: Vec<PgAuthIdRow>,
        settable_roles: Vec<u32>,
    }

    impl CreateSchemaAuthContext for TestAuth {
        fn current_user_oid(&self) -> u32 {
            self.current_user_oid
        }

        fn session_user_oid(&self) -> u32 {
            self.session_user_oid
        }

        fn role_by_oid(&self, oid: u32) -> Option<&PgAuthIdRow> {
            self.roles.iter().find(|role| role.oid == oid)
        }

        fn role_by_name(&self, name: &str) -> Option<&PgAuthIdRow> {
            self.roles
                .iter()
                .find(|role| role.rolname.eq_ignore_ascii_case(name))
        }

        fn can_set_role(&self, role_oid: u32) -> bool {
            self.settable_roles.contains(&role_oid)
        }
    }

    fn role(oid: u32, name: &str, superuser: bool) -> PgAuthIdRow {
        PgAuthIdRow {
            oid,
            rolname: name.into(),
            rolsuper: superuser,
            rolinherit: true,
            rolcreaterole: false,
            rolcreatedb: false,
            rolcanlogin: false,
            rolreplication: false,
            rolbypassrls: false,
            rolconnlimit: -1,
            rolpassword: None,
            rolvaliduntil: None,
        }
    }

    fn stmt(schema_name: Option<&str>, auth_role: Option<RoleSpec>) -> CreateSchemaStatement {
        CreateSchemaStatement {
            schema_name: schema_name.map(str::to_string),
            auth_role,
            if_not_exists: false,
            elements: Vec::new(),
        }
    }

    #[test]
    fn create_schema_defaults_name_to_owner_role() {
        let auth = TestAuth {
            current_user_oid: 11,
            session_user_oid: 11,
            roles: vec![role(11, "alice", true)],
            settable_roles: Vec::new(),
        };
        assert_eq!(
            resolve_create_schema_stmt(&stmt(None, None), &auth, 11, false, &[]).unwrap(),
            CreateSchemaResolution::Create(ResolvedCreateSchema {
                schema_name: "alice".into(),
                owner_oid: 11,
            })
        );
    }

    #[test]
    fn create_schema_requires_set_role_for_different_owner() {
        let auth = TestAuth {
            current_user_oid: 11,
            session_user_oid: 11,
            roles: vec![role(11, "alice", true), role(12, "bob", false)],
            settable_roles: Vec::new(),
        };
        assert!(matches!(
            resolve_create_schema_stmt(
                &stmt(Some("s"), Some(RoleSpec::RoleName("bob".into()))),
                &auth,
                11,
                false,
                &[],
            ),
            Err(ExprError::DetailedError {
                sqlstate: "42501",
                ..
            })
        ));
    }

    #[test]
    fn create_schema_if_not_exists_skips_existing_schema() {
        let auth = TestAuth {
            current_user_oid: 11,
            session_user_oid: 11,
            roles: vec![role(11, "alice", true)],
            settable_roles: Vec::new(),
        };
        let mut create = stmt(Some("app"), None);
        create.if_not_exists = true;
        let namespaces = vec![PgNamespaceRow {
            oid: 1,
            nspname: "app".into(),
            nspowner: 11,
            nspacl: None,
        }];
        assert_eq!(
            resolve_create_schema_stmt(&create, &auth, 11, false, &namespaces).unwrap(),
            CreateSchemaResolution::SkipExisting("app".into())
        );
    }
}
