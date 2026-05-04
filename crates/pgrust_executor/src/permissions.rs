use pgrust_catalog_data::{BOOTSTRAP_SUPERUSER_OID, PgAuthIdRow, PgAuthMembersRow, PgClassRow};

pub trait PermissionCatalog {
    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow>;
    fn authid_rows(&self) -> Vec<PgAuthIdRow>;
    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow>;
}

pub fn relation_values_visible_for_error_detail(
    relation_oid: u32,
    current_user_oid: u32,
    catalog: Option<&dyn PermissionCatalog>,
) -> bool {
    if current_user_oid == BOOTSTRAP_SUPERUSER_OID {
        return true;
    }
    let Some(catalog) = catalog else {
        return true;
    };
    let Some(class_row) = catalog.class_row_by_oid(relation_oid) else {
        return true;
    };
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();

    // :HACK: Match PostgreSQL's error-detail leak guard for RLS and
    // table-level SELECT. Once pg_attribute.attacl is modeled, this should
    // also allow users with SELECT on every displayed key column.
    !pgrust_catalog_store::relation_has_enabled_rls_for_user(
        &class_row,
        &authid_rows,
        &auth_members_rows,
        current_user_oid,
    ) && pgrust_catalog_store::relation_has_table_select_privilege(
        &class_row,
        &authid_rows,
        &auth_members_rows,
        current_user_oid,
    )
}
