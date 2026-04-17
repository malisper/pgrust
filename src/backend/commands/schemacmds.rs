use crate::backend::executor::ExecError;
use crate::backend::parser::CreateSchemaStatement;
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
