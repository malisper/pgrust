use crate::backend::catalog::CatalogError;
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgAuthIdRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAttributes {
    pub rolsuper: bool,
    pub rolinherit: bool,
    pub rolcreaterole: bool,
    pub rolcreatedb: bool,
    pub rolcanlogin: bool,
    pub rolreplication: bool,
    pub rolbypassrls: bool,
    pub rolconnlimit: i32,
}

impl Default for RoleAttributes {
    fn default() -> Self {
        Self {
            rolsuper: false,
            rolinherit: true,
            rolcreaterole: false,
            rolcreatedb: false,
            rolcanlogin: false,
            rolreplication: false,
            rolbypassrls: false,
            rolconnlimit: -1,
        }
    }
}

pub fn bootstrap_predefined_roles(rows: &[PgAuthIdRow]) -> Vec<PgAuthIdRow> {
    rows.iter()
        .filter(|row| row.oid != BOOTSTRAP_SUPERUSER_OID)
        .cloned()
        .collect()
}

pub fn find_role_by_name<'a>(rows: &'a [PgAuthIdRow], role_name: &str) -> Option<&'a PgAuthIdRow> {
    rows.iter()
        .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
}

pub fn find_role_by_oid(rows: &[PgAuthIdRow], oid: u32) -> Option<&PgAuthIdRow> {
    rows.iter().find(|row| row.oid == oid)
}

pub fn create_role(
    rows: &mut Vec<PgAuthIdRow>,
    next_oid: &mut u32,
    role_name: impl Into<String>,
    attrs: &RoleAttributes,
) -> Result<PgAuthIdRow, CatalogError> {
    let role_name = role_name.into();
    if find_role_by_name(rows, &role_name).is_some() {
        return Err(CatalogError::UniqueViolation(format!(
            "duplicate role name: {role_name}"
        )));
    }
    let row = PgAuthIdRow {
        oid: *next_oid,
        rolname: role_name,
        rolsuper: attrs.rolsuper,
        rolinherit: attrs.rolinherit,
        rolcreaterole: attrs.rolcreaterole,
        rolcreatedb: attrs.rolcreatedb,
        rolcanlogin: attrs.rolcanlogin,
        rolreplication: attrs.rolreplication,
        rolbypassrls: attrs.rolbypassrls,
        rolconnlimit: attrs.rolconnlimit,
    };
    *next_oid = next_oid.saturating_add(1);
    rows.push(row.clone());
    rows.sort_by_key(|existing| (existing.oid, existing.rolname.clone()));
    Ok(row)
}

pub fn alter_role_attributes(
    rows: &mut [PgAuthIdRow],
    role_name: &str,
    attrs: &RoleAttributes,
) -> Result<PgAuthIdRow, CatalogError> {
    let row = rows
        .iter_mut()
        .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
        .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
    row.rolsuper = attrs.rolsuper;
    row.rolinherit = attrs.rolinherit;
    row.rolcreaterole = attrs.rolcreaterole;
    row.rolcreatedb = attrs.rolcreatedb;
    row.rolcanlogin = attrs.rolcanlogin;
    row.rolreplication = attrs.rolreplication;
    row.rolbypassrls = attrs.rolbypassrls;
    row.rolconnlimit = attrs.rolconnlimit;
    Ok(row.clone())
}

pub fn rename_role(
    rows: &mut [PgAuthIdRow],
    role_name: &str,
    new_name: &str,
) -> Result<PgAuthIdRow, CatalogError> {
    if rows.iter().any(|row| {
        row.rolname.eq_ignore_ascii_case(new_name) && !row.rolname.eq_ignore_ascii_case(role_name)
    }) {
        return Err(CatalogError::UniqueViolation(format!(
            "duplicate role name: {new_name}"
        )));
    }
    let row = rows
        .iter_mut()
        .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
        .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
    row.rolname = new_name.to_string();
    Ok(row.clone())
}

pub fn drop_roles(
    rows: &mut Vec<PgAuthIdRow>,
    role_names: &[String],
) -> Result<Vec<PgAuthIdRow>, CatalogError> {
    let mut removed = Vec::new();
    for role_name in role_names {
        let Some(idx) = rows
            .iter()
            .position(|row| row.rolname.eq_ignore_ascii_case(role_name))
        else {
            return Err(CatalogError::UnknownTable(role_name.clone()));
        };
        removed.push(rows.remove(idx));
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_rename_role_rows() {
        let mut rows = Vec::new();
        let mut next_oid = 100;
        let created = create_role(
            &mut rows,
            &mut next_oid,
            "app_user",
            &RoleAttributes {
                rolcanlogin: true,
                ..RoleAttributes::default()
            },
        )
        .unwrap();
        assert_eq!(created.oid, 100);
        assert!(created.rolcanlogin);
        assert_eq!(
            find_role_by_name(&rows, "app_user").map(|row| row.oid),
            Some(100)
        );

        let renamed = rename_role(&mut rows, "app_user", "app_owner").unwrap();
        assert_eq!(renamed.rolname, "app_owner");
        assert!(find_role_by_name(&rows, "app_user").is_none());
        assert!(find_role_by_name(&rows, "app_owner").is_some());
    }

    #[test]
    fn duplicate_role_names_are_rejected() {
        let mut rows = Vec::new();
        let mut next_oid = 100;
        create_role(
            &mut rows,
            &mut next_oid,
            "app_user",
            &RoleAttributes::default(),
        )
        .unwrap();
        let err = create_role(
            &mut rows,
            &mut next_oid,
            "app_user",
            &RoleAttributes::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CatalogError::UniqueViolation(_)));
    }
}
